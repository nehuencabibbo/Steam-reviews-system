import logging
import signal
from typing import * 
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import (
    read,
    read_by_range,
    save,
    write_batch_by_range_per_client,
    delete_directory,
    delete_file,
    atomically_append_to_file,
)
from common.activity_log.activity_log import ActivityLog
from utils.utils import group_batch_by_field, node_id_to_send_to

APP_ID_INDEX = 1
END_TRANSMISSION_MESSAGE = "END"


class Join:
    def __init__(
        self, 
        middleware: Middleware, 
        config: dict[str, str],
        activity_log: ActivityLog,
    ):
        self.__middleware = middleware
        self._amount_of_games_ends_recived = {}
        self._amount_of_reviews_ends_recived = {}
        self._got_sigterm = False
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

        self._activity_log = activity_log

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def __signal_handler(self, sig, frame):
        logging.debug(f"Gracefully shutting down...")
        self._got_sigterm = True
        self.__middleware.shutdown()
        # self.__middleware.stop_consuming_gracefully()

    def start(self):
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

        try:
            self.__middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self.__middleware.shutdown()

    def __games_callback(self, delivery_tag, body, message_type, forwarding_queue_name):
        body = self.__middleware.get_rows_from_message(body)

        if len(body) == 1 and body[0][1] == END_TRANSMISSION_MESSAGE:
            client_id = body[0][0]
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

                if not client_id in self._amount_of_reviews_ends_recived:
                    self._amount_of_reviews_ends_recived[client_id] = 0

                if (
                    self._amount_of_reviews_ends_recived[client_id]
                    == self._needed_reviews_ends
                ):
                    self.__send_end_to_forward_queues(client_id)
                    self.__clear_client_data(client_id)

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
                APP_ID_INDEX
            )

        except ValueError as e:
            logging.error(
                f"An error has occurred. {e}",
            )

        self.__middleware.ack(delivery_tag)

    def __reviews_callback(
        self, delivery_tag, body, message_type, forwarding_queue_name
    ):
        body = self.__middleware.get_rows_from_message(body)
        for review in body:
            logging.debug(f"Recived review: {review}")
            client_id = review.pop(0)

            if not client_id in self._amount_of_games_ends_recived:
                self._amount_of_games_ends_recived[client_id] = 0

            if len(review) == 1 and review[0] == END_TRANSMISSION_MESSAGE:

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

                self.__middleware.ack(delivery_tag)

                return

            # Here we check for games ENDs, NOT for reviews
            if not (
                self._amount_of_games_ends_recived[client_id] == self._needed_games_ends
            ):
                # Havent received all ends, save in disk    
                atomically_append_to_file('tmp', f'reviews_{client_id}.csv', [review])
            else:
                self.__join_and_send(review, client_id, forwarding_queue_name)

        # Cayo aca
        self.__middleware.ack(delivery_tag)

    def __send_end_to_forward_queues(self, client_id: str):
        forwarding_queue_name = self._output_queue_name

        if (
            "Q" in forwarding_queue_name
        ):  # gotta check this as it could be the last node, then a prefix shouldn't be used
            self.__middleware.publish_batch(forwarding_queue_name)
            end_message = [client_id, "END", f"{self._instances_of_myself}"]
            self.__middleware.send_end(forwarding_queue_name, end_message=end_message)
            logging.debug(f"Sent end to: {forwarding_queue_name}")
            return

        for i in range(self._amount_of_forwarding_queues):
            self.__middleware.publish_batch(f"{i}_{forwarding_queue_name}")
            self.__middleware.send_end(
                f"{i}_{forwarding_queue_name}",
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )
            logging.debug(f"Sent end to: {i}_{forwarding_queue_name}")

    def __games_columns_to_keep(self, games_record: list[str]):
        return [games_record[i] for i in self._games_columns_to_keep]

    def __reviews_columns_to_keep(self, reviews_record: list[str]):
        return [reviews_record[i] for i in self._reviews_columns_to_keep]

    def __send_stored_reviews(self, client_id, forwarding_queue_name):
        for record in read(f"tmp/reviews_{client_id}.csv"):
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
        self._amount_of_games_ends_recived.pop(client_id)
        self._amount_of_reviews_ends_recived.pop(client_id)
