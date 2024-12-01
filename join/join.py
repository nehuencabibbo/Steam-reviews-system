import logging
import os
import signal
import threading

from time import sleep
from typing import * 
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import (
    delete_directory,
    read,
    read_by_range,
    write_batch_by_range_per_client,
    atomically_append_to_file,
)
from common.activity_log.activity_log import ActivityLog
from utils.utils import node_id_to_send_to
from common.watchdog_client.watchdog_client import WatchdogClient


REGULAR_MESSAGE_CLIENT_ID_INDEX = 0
REGULAR_MESSAGE_APP_ID_INDEX = 1

END_TRANSMISSION_MESSAGE = "END"
SESSION_TIMEOUT_MESSAGE = "TIMEOUT"

END_TRANSMISSION_MESSAGE_CLIENT_ID_INDEX = 0
END_TRANSMISSION_MESSAGE_MSG_ID_INDEX = 1
END_TRANSMISSION_MESSAGE_END_INDEX = 2

GAMES_END_LOGGING = 0
REVIEWS_END_LOGGING = 1

class Join:
    def __init__(
        self, 
        middleware: Middleware, 
        config: dict[str, str],
        activity_log: ActivityLog,
        monitor: WatchdogClient
    ):
        self.__middleware = middleware
        self._amount_of_games_ends_recived = {}
        self._amount_of_reviews_ends_recived = {}
        self._amount_of_timeouts_per_client_received = {}
        self._got_sigterm = False
        self._client_monitor = monitor

        self._input_games_queue_name = config["INPUT_GAMES_QUEUE_NAME"]
        self._input_reviews_queue_name = config["INPUT_REVIEWS_QUEUE_NAME"]
        self._output_queue_name = config["OUTPUT_QUEUE_NAME"]
        self._amount_of_forwarding_queues = config["AMOUNT_OF_FORWARDING_QUEUES"]
        self._needed_games_ends = config["NEEDED_GAMES_ENDS"]
        self._needed_reviews_ends = config["NEEDED_REVIEWS_ENDS"]
        self._partition_range = config["PARTITION_RANGE"]
        self._instances_of_myself = config["INSTANCES_OF_MYSELF"]
        self._games_columns_to_keep = config["GAMES_COLUMNS_TO_KEEP"]
        self._reviews_columns_to_keep = config["REVIEWS_COLUMNS_TO_KEEP"]
        self._needed_timeouts = self._needed_games_ends + self._needed_reviews_ends
        self._node_id = config["NODE_ID"]

        self._activity_log = activity_log
        
        self._amount_of_games_ends_recived, self._amount_of_reviews_ends_recived = self._activity_log.recover_ends_state()

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def __resume_publish_if_necesary(self):
        logging.debug('[RECOVERY] Recovering ends')
        logging.debug(f'[RECOVERY] GAMES: {self._amount_of_games_ends_recived}')
        logging.debug(f'[RECOVERY] REVIEWS: {self._amount_of_games_ends_recived}')
        client_ids_to_remove = []
        for client_id, amount in self._amount_of_games_ends_recived.items():
            if amount == self._needed_reviews_ends:
                # If all games arrived, send all remaining reviews
                logging.debug(f'[RECOVERY] Sending all stored reviews to {self._output_queue_name}')
                self.__send_stored_reviews(
                    client_id, 
                    forwarding_queue_name=self._output_queue_name
                )
                # If all reviews arrived too for that client, the forward end and remove everything
                if self._amount_of_reviews_ends_recived.get(client_id, 0) == self._needed_reviews_ends:
                    self.__send_end_to_forward_queues(client_id)
                    client_ids_to_remove.append(client_id)

        for client_id in client_ids_to_remove:
            logging.debug(f'[RECOVERY] deleting {client_id} directory')
            self.__clear_client_data(client_id)
            logging.debug(f'[RECOVERY] deleting {client_id} logs')
            self._activity_log.remove_client_logs(client_id)


    def __signal_handler(self, sig, frame):
        logging.debug(f"Gracefully shutting down...")
        self._got_sigterm = True
        self.__middleware.shutdown()
        self._client_monitor.stop()
        #self.__middleware.stop_consuming_gracefully()

    def start(self):

        monitor_thread = threading.Thread(target=self._client_monitor.start)
        monitor_thread.start()

        # gotta check this as it could be the last node, then a prefix shouldn't be used
        self.__middleware.create_queue(self._input_games_queue_name)
        self.__middleware.create_queue(self._input_reviews_queue_name)

        if not "Q" in self._output_queue_name:
            for i in range(self._amount_of_forwarding_queues):
                self.__middleware.create_queue(f"{i}_{self._output_queue_name}")

        # callback, inputq, outputq
        games_callback = self.__middleware.generate_callback(
            self.__games_callback, self._input_games_queue_name, self._output_queue_name
        )

        self.__middleware.attach_callback(self._input_games_queue_name, games_callback)

        reviews_callback = self.__middleware.generate_callback(
            self.__reviews_callback,
            self._input_reviews_queue_name,
            self._output_queue_name,
        )

        self.__middleware.attach_callback(
            self._input_reviews_queue_name, reviews_callback
        )

        self.__resume_publish_if_necesary()
        try:
            self.__middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self.__middleware.shutdown()
            monitor_thread.join()

    def __games_callback(self, delivery_tag, body, message_type, forwarding_queue_name):
        # logging.debug(f'[CHECK] {message_type}')
        # logging.debug(f'[CHECK] {forwarding_queue_name}')
        body = self.__middleware.get_rows_from_message(body)

        if len(body) == 1 and body[0][1] == SESSION_TIMEOUT_MESSAGE:
            session_id = body[0][0]
            self._amount_of_timeouts_per_client_received[session_id] = (
                self._amount_of_timeouts_per_client_received.get(session_id, 0) + 1
            )

            logging.debug(
                (
                    f"Amount of timeouts received up to now for client: {session_id}: {self._amount_of_timeouts_per_client_received[session_id]}"
                    f"| Expecting: {self._needed_timeouts}"
                )
            )

            if (
                self._amount_of_timeouts_per_client_received[session_id]
                == self._needed_timeouts
            ):
                self.__send_to_forward_queues(
                    session_id, message=SESSION_TIMEOUT_MESSAGE
                )
                self.__clear_client_data(session_id)

            self.__middleware.ack(delivery_tag)

            return

        if len(body) == 1 and body[0][END_TRANSMISSION_MESSAGE_END_INDEX] == END_TRANSMISSION_MESSAGE:
            client_id = body[0][END_TRANSMISSION_MESSAGE_CLIENT_ID_INDEX]
            msg_id = body[0][END_TRANSMISSION_MESSAGE_MSG_ID_INDEX]

            self.__handle_games_end_transmission(client_id, msg_id, forwarding_queue_name)

            self.__middleware.ack(delivery_tag)

            return

        try:
            # Si en el group by de un cliente hay al menos un mensaje
            # que ya fue procesado, eso quiere decir que todos los mensajes
            # de ese cliente ya fueron procesados, por lo tanto los descarto
            # y proceso los que me quedan
            write_batch_by_range_per_client(
                "tmp/", 
                int(self._partition_range), 
                body,
                REGULAR_MESSAGE_APP_ID_INDEX
            )

        except ValueError as e:
            logging.error(
                f"An error has occurred. {e}",
            )

        self.__middleware.ack(delivery_tag)

    def __handle_games_end_transmission(self, client_id: str, msg_id: str, forwarding_queue_name: str):
        # TODO: VERIFICAR QUE ESTO ESTE BIEN
        client_dir = os.path.join('tmp', client_id)
        if not os.path.exists(client_dir):
            logging.debug('Recived GAMES END, but client directory was already deleted. Propagating END')
            self.__send_end_to_forward_queues(client_id)

            return

        was_duplicate = self._activity_log.log_end(client_id, msg_id, end_logging=GAMES_END_LOGGING)
        if was_duplicate: return

        self._amount_of_games_ends_recived[client_id] = (
            self._amount_of_games_ends_recived.get(client_id, 0) + 1
        )
        logging.debug(
            (
                f"Amount of games ends received up to now: {self._amount_of_games_ends_recived[client_id]}"
                f"| Expecting: {self._needed_games_ends}"
            )
        )
        if self._amount_of_games_ends_recived[client_id] == self._needed_games_ends:
            if "Q" in forwarding_queue_name:
                # Created here, otherwise each review that is joined must created the queue
                self.__middleware.create_queue(forwarding_queue_name)

            self.__send_stored_reviews(
                client_id, forwarding_queue_name=forwarding_queue_name
            )

            if (
                client_id in self._amount_of_reviews_ends_recived and 
                self._amount_of_reviews_ends_recived[client_id] == self._needed_reviews_ends
            ):
                self.__send_end_to_forward_queues(client_id)
                self.__clear_client_data(client_id)


    def __reviews_callback(
        self, delivery_tag, body, message_type, forwarding_queue_name
    ):
        # TODO: Hacer que quede consistente lo del client_id
        body = self.__middleware.get_rows_from_message(body)
        for review in body:
            logging.debug(f"Recived review: {review}")
            # TODO: Fix both constants
            client_id = review[END_TRANSMISSION_MESSAGE_CLIENT_ID_INDEX]
            
            if len(review) == 1 and review[1] == SESSION_TIMEOUT_MESSAGE: 
                self._amount_of_timeouts_per_client_received[client_id] = (
                    self._amount_of_timeouts_per_client_received.get(client_id, 0) + 1
                )

                logging.info(
                    (
                        f"Amount of timeouts received up to now for client: {client_id}: {self._amount_of_timeouts_per_client_received[client_id]}"
                        f"| Expecting: {self._needed_timeouts}"
                    )
                )

                if (
                    self._amount_of_timeouts_per_client_received[client_id]
                    == self._needed_timeouts
                ):
                    self.__send_to_forward_queues(
                        client_id, message=SESSION_TIMEOUT_MESSAGE
                    )
                    self.__clear_client_data(client_id)

                self.__middleware.ack(delivery_tag)

                return


            msg_id = review[END_TRANSMISSION_MESSAGE_MSG_ID_INDEX]
            if review[END_TRANSMISSION_MESSAGE_END_INDEX] == END_TRANSMISSION_MESSAGE:
                
                self.__handle_reviews_end_transmission(client_id, msg_id)

                self.__middleware.ack(delivery_tag)

                return

            client_id = review.pop(REGULAR_MESSAGE_CLIENT_ID_INDEX) 
            # TODO: Sacar este pop, pasar la review completa y usar las ctes
            # Here we check for games ENDs, NOT for reviews
            if not (
                self._amount_of_games_ends_recived[client_id] == self._needed_games_ends
            ):
                # Havent received all ends, save in disk
                client_dir = os.path.join('tmp', client_id)
                os.makedirs(client_dir, exist_ok=True)
                atomically_append_to_file(client_dir, 'reviews.csv', [review])
            else:
                self.__join_and_send(review, client_id, forwarding_queue_name)

        self.__middleware.ack(delivery_tag)

    def __handle_reviews_end_transmission(self, client_id: str, msg_id: str): 
        # TODO: VERIFICAR QUE ESTO ESTE BIEN
        client_dir = os.path.join('tmp', client_id)
        if not os.path.exists(client_dir):
            logging.debug('Recived REVIEWS END, but client directory was already deleted. Propagating END')
            self.__send_end_to_forward_queues(client_id)

            return
        
        was_duplicate = self._activity_log.log_end(client_id, msg_id, end_logging=REVIEWS_END_LOGGING)
        if was_duplicate: return 
        self._amount_of_reviews_ends_recived[client_id] = (
            self._amount_of_reviews_ends_recived.get(client_id, 0) + 1
        )
        logging.debug("END of reviews received")
        logging.debug(
            f"Amount of reviews ends received up to now: {self._amount_of_reviews_ends_recived[client_id]} | Expecting: {self._needed_reviews_ends}"
        )

        if (
            self._amount_of_reviews_ends_recived[client_id]
            == self._needed_reviews_ends
            and self._amount_of_games_ends_recived[client_id]
            == self._needed_games_ends
        ):
            self.__send_end_to_forward_queues(client_id)
            self.__clear_client_data(client_id)


    def __send_end_to_forward_queues(self, client_id: str):
        forwarding_queue_name = self._output_queue_name

        if (
            "Q" in forwarding_queue_name
        ):  # gotta check this as it could be the last node, then a prefix shouldn't be used
            self.__middleware.publish_batch(forwarding_queue_name)

            # TODO: Si hay mas de un agregador hay un problema? por el self._instances_of_myself
            end_message = [client_id, self._node_id, "END", f"{self._instances_of_myself}"]
            self.__middleware.send_end(forwarding_queue_name, end_message=end_message)
            logging.debug(f"Sent end to: {forwarding_queue_name}")

            return

        for i in range(self._amount_of_forwarding_queues):
            self.__middleware.publish_batch(f"{i}_{forwarding_queue_name}")
            self.__middleware.send_end(
                f"{i}_{forwarding_queue_name}",
                end_message=[client_id, self._node_id, END_TRANSMISSION_MESSAGE],
            )
            logging.debug(f"Sent end to: {i}_{forwarding_queue_name}")

    def __games_columns_to_keep(self, games_record: list[str]):
        return [games_record[i] for i in self._games_columns_to_keep]

    def __reviews_columns_to_keep(self, reviews_record: list[str]):
        return [reviews_record[i] for i in self._reviews_columns_to_keep]

    def __send_stored_reviews(self, client_id, forwarding_queue_name):
        for record in read(f"tmp/{client_id}/reviews.csv"):
            self.__join_and_send(
                review=record,
                client_id=client_id,
                forwarding_queue_name=forwarding_queue_name,
            )

    @staticmethod
    def __generate_unique_msg_id(game_msg_id: str, review_msg_id: str) -> str:
        '''
        games_msg_id > reviews_msg_id 
        '''
        return  str(len(game_msg_id)) + game_msg_id + str(len(review_msg_id)) + review_msg_id

    def __join_and_send(self, review, client_id, forwarding_queue_name):
        # TODO: handle conversion error
        review_msg_id = review[0]
        review_app_id = int(review[1])
        for record in read_by_range(
            f"tmp/{client_id}", int(self._partition_range), review_app_id
        ):
            # record_splitted = record.split(",", maxsplit=1)
            record_msg_id = record[0]
            record_app_id = record[1]
            logging.debug(f'record_msg_id: {record_msg_id} | review_msg_id: {review_msg_id}')
            logging.debug(f'record_app_id: {record_app_id} | review_app_id: {record_app_id}')
            if review_app_id == int(record_app_id):
                joined_message = [
                    client_id, 
                    # New id that's generated is just the concatenation of 
                    # both previous ids
                    Join.__generate_unique_msg_id(record_msg_id, str(review_msg_id)),
                ] 

                joined_message += self.__games_columns_to_keep(record) 
                joined_message += self.__reviews_columns_to_keep(review)

                if "Q" in forwarding_queue_name: 
                    # gotta check this as it could be the last node, then a prefix shouldn't be used
                    logging.debug(
                        f"Q - Sending {joined_message} to queue {forwarding_queue_name}"
                    )
                    self.__middleware.publish(
                        joined_message,
                        forwarding_queue_name,
                    )

                else:

                    node_id = node_id_to_send_to(
                        client_id,
                        record_app_id,
                        self._amount_of_forwarding_queues,
                    )

                    logging.debug(
                        f"Sending message: {joined_message} to queue: {node_id}_{forwarding_queue_name}"
                    )

                    self.__middleware.publish(
                        joined_message,
                        f"{node_id}_{forwarding_queue_name}",
                    )

    def __clear_client_data(self, client_id: str):
        # delete_directory(f"/tmp/{client_id}")
        # delete_file(f"/tmp/reviews_{client_id}.csv")
        client_dir = f'/tmp/{client_id}'
        if not delete_directory(client_dir):
            logging.debug(f"Couldn't delete directory: {client_dir}")
        else:
            logging.debug(f"Deleted directory: {client_dir}")

        if client_id in self._amount_of_games_ends_recived:
            del self._amount_of_games_ends_recived[client_id]
        if client_id in self._amount_of_reviews_ends_recived:
            del self._amount_of_reviews_ends_recived[client_id]
        if client_id in self._amount_of_timeouts_per_client_received:
            del self._amount_of_timeouts_per_client_received[client_id]

