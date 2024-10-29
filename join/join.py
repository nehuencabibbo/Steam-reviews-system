import logging
import signal
from time import sleep
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import (
    read,
    read_by_range,
    save,
    write_batch_by_range_per_client,
    delete_directory,
    delete_file,
)
from common.protocol.protocol import Protocol
from utils.utils import node_id_to_send_to

END_TRANSMISSION_MESSAGE = "END"


class Join:
    def __init__(
        self, protocol: Protocol, middleware: Middleware, config: dict[str, str]
    ):
        self.__middleware = middleware
        self.__config = config
        self._amount_of_games_ends_recived = {}
        self._amount_of_reviews_ends_recived = {}
        self._got_sigterm = False

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def __signal_handler(self, sig, frame):
        logging.debug(f"Gracefully shutting down...")
        self._got_sigterm = True
        self.__middleware.shutdown()

    def start(self):
        # gotta check this as it could be the last node, then a prefix shouldn't be used
        self.__middleware.create_queue(self.__config["INPUT_GAMES_QUEUE_NAME"])
        self.__middleware.create_queue(self.__config["INPUT_REVIEWS_QUEUE_NAME"])

        if not "Q" in self.__config["OUTPUT_QUEUE_NAME"]:
            for i in range(self.__config["AMOUNT_OF_FORWARDING_QUEUES"]):
                self.__middleware.create_queue(
                    f'{i}_{self.__config["OUTPUT_QUEUE_NAME"]}'
                )

        # callback, inputq, outputq
        games_callback = self.__middleware.generate_callback(
            self.__games_callback,
            self.__config["INPUT_GAMES_QUEUE_NAME"],
            self.__config["OUTPUT_QUEUE_NAME"],
        )

        self.__middleware.attach_callback(
            self.__config["INPUT_GAMES_QUEUE_NAME"], games_callback
        )

        reviews_callback = self.__middleware.generate_callback(
            self.__reviews_callback,
            self.__config["INPUT_REVIEWS_QUEUE_NAME"],
            self.__config["OUTPUT_QUEUE_NAME"],
        )

        self.__middleware.attach_callback(
            self.__config["INPUT_REVIEWS_QUEUE_NAME"], reviews_callback
        )

        try:
            self.__middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)

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
                    f"| Expecting: {self.__config['NEEDED_GAMES_ENDS']}"
                )
            )
            if (
                self._amount_of_games_ends_recived[client_id]
                == self.__config["NEEDED_GAMES_ENDS"]
            ):
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
                    == self.__config["NEEDED_REVIEWS_ENDS"]
                ):
                    self.__send_end_to_forward_queues(client_id)
                    self.__clear_client_data(client_id)

            self.__middleware.ack(delivery_tag)

            return

        logging.debug(f"Recived game: {body}")
        try:
            write_batch_by_range_per_client(
                "tmp/", int(self.__config["PARTITION_RANGE"]), body
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
                # send rest of batch if there is any
                # self.__middleware.publish_batch(forwarding_queue_name)

                # if not delete_directory('/tmp'):
                #     logging.debug(f"Couldn't delete directory: {'/tmp'}")
                # else:
                #     logging.debug(f"Deleted directory: {'/tmp'}")

                logging.info("END of reviews received")
                logging.debug(
                    f"Amount of reviews ends received up to now: {self._amount_of_reviews_ends_recived[client_id]} | Expecting: {self.__config['NEEDED_REVIEWS_ENDS']}"
                )

                if (
                    self._amount_of_reviews_ends_recived[client_id]
                    == self.__config["NEEDED_REVIEWS_ENDS"]
                    and self._amount_of_games_ends_recived[client_id]
                    == self.__config["NEEDED_GAMES_ENDS"]
                ):
                    self.__send_end_to_forward_queues(client_id)
                    self.__clear_client_data(client_id)

                self.__middleware.ack(delivery_tag)

                return

            # Here we check for games ENDs, NOT for reviews
            if not (
                self._amount_of_games_ends_recived[client_id]
                == self.__config["NEEDED_GAMES_ENDS"]
            ):
                # Havent received all ends, save in disk
                save(f"tmp/reviews_{client_id}.csv", review)
            else:
                self.__join_and_send(review, client_id, forwarding_queue_name)

        self.__middleware.ack(delivery_tag)

    def __send_end_to_forward_queues(self, client_id: str):
        forwarding_queue_name = self.__config["OUTPUT_QUEUE_NAME"]

        if (
            "Q" in forwarding_queue_name
        ):  # gotta check this as it could be the last node, then a prefix shouldn't be used
            self.__middleware.publish_batch(forwarding_queue_name)
            end_message = [client_id, "END", f'{self.__config["INSTANCES_OF_MYSELF"]}']
            self.__middleware.send_end(forwarding_queue_name, end_message=end_message)
            logging.debug(f"Sent end to: {forwarding_queue_name}")
            return

        for i in range(self.__config["AMOUNT_OF_FORWARDING_QUEUES"]):
            self.__middleware.publish_batch(f"{i}_{forwarding_queue_name}")
            self.__middleware.send_end(
                f"{i}_{forwarding_queue_name}",
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )
            logging.debug(f"Sent end to: {i}_{forwarding_queue_name}")

    def __games_columns_to_keep(self, games_record: list[str]):
        return [games_record[i] for i in self.__config["GAMES_COLUMNS_TO_KEEP"]]

    def __reviews_columns_to_keep(self, reviews_record: list[str]):
        return [reviews_record[i] for i in self.__config["REVIEWS_COLUMNS_TO_KEEP"]]

    def __send_stored_reviews(self, client_id, forwarding_queue_name):
        for record in read(f"tmp/reviews_{client_id}.csv"):
            self.__join_and_send(
                review=record,
                client_id=client_id,
                forwarding_queue_name=forwarding_queue_name,
            )

    def __join_and_send(self, review, client_id, forwarding_queue_name):
        # TODO: handle conversion error
        app_id = int(review[0])

        for record in read_by_range(
            f"tmp/{client_id}", int(self.__config["PARTITION_RANGE"]), app_id
        ):
            # record_splitted = record.split(",", maxsplit=1)
            record_app_id = record[0]
            logging.debug(f"app_id: {app_id} | record_app_id: {record_app_id}")
            if app_id == int(record_app_id):
                # Get rid of the app_id from the review and append it to the original game record
                # TODO: QUE NO HAGA UNA LISTA!!
                # joined_message = [record_app_id, record_info] + review[1:]
                joined_message = self.__games_columns_to_keep(
                    record
                ) + self.__reviews_columns_to_keep(review)

                joined_message.insert(0, client_id)

                if (
                    "Q" in forwarding_queue_name
                ):  # gotta check this as it could be the last node, then a prefix shouldn't be used
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
                        self.__config["AMOUNT_OF_FORWARDING_QUEUES"],
                    )

                    logging.debug(
                        f"Sending message: {joined_message} to queue: {node_id}_{forwarding_queue_name}"
                    )

                    self.__middleware.publish(
                        joined_message,
                        f"{node_id}_{forwarding_queue_name}",
                    )
                    
    def __clear_client_data(self, client_id: str):

        delete_directory(f"/tmp/{client_id}")
        delete_file(f"/tmp/reviews_{client_id}.csv")
        self._amount_of_games_ends_recived.pop(client_id)
        self._amount_of_reviews_ends_recived.pop(client_id)