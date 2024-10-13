import logging
import signal
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import read_by_range, write_by_range, delete_directory
from common.protocol.protocol import Protocol
from utils.utils import node_id_to_send_to

END_TRANSMISSION_MESSAGE = "END"


class Join:
    def __init__(
        self, protocol: Protocol, middleware: Middleware, config: dict[str, str]
    ):
        self.__middleware = middleware
        self.__config = config
        self._amount_of_ends_received = 0
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
        else:
            self.__middleware.create_queue(self.__config["OUTPUT_QUEUE_NAME"])

        # callback, inputq, outputq
        games_callback = self.__middleware.generate_callback(
            self.__games_callback,
            self.__config["INPUT_GAMES_QUEUE_NAME"],
            self.__config["OUTPUT_QUEUE_NAME"],
        )

        self.__middleware.attach_callback(
            self.__config["INPUT_GAMES_QUEUE_NAME"], games_callback
        )

        try:
            self.__middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)

    def __games_callback(self, delivery_tag, body, message_type, forwarding_queue_name):
        body = self.__middleware.get_rows_from_message(body)
        for message in body:
            logging.debug(f"Recived game: {message}")
            if len(message) == 1 and message[0] == END_TRANSMISSION_MESSAGE:
                logging.debug("END of games received")
                reviews_callback = self.__middleware.generate_callback(
                    self.__reviews_callback,
                    self.__config["INPUT_REVIEWS_QUEUE_NAME"],
                    self.__config["OUTPUT_QUEUE_NAME"],
                )

                self.__middleware.attach_callback(
                    self.__config["INPUT_REVIEWS_QUEUE_NAME"], reviews_callback
                )
                self.__middleware.ack(delivery_tag)

                return

            try:
                write_by_range(
                    "tmp/", int(self.__config["PARTITION_RANGE"]), ",".join(message)
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

            if len(review) == 1 and review[0] == END_TRANSMISSION_MESSAGE:

                # send rest of batch if there is any
                # self.__middleware.publish_batch(forwarding_queue_name)

                # if not delete_directory('/tmp'):
                #     logging.debug(f"Couldn't delete directory: {'/tmp'}")
                # else:
                #     logging.debug(f"Deleted directory: {'/tmp'}")

                logging.debug("END of reviews received")
                self._amount_of_ends_received += 1
                logging.debug(
                    f"Amount of ends received up to now: {self._amount_of_ends_received} | Expecting: {self.__config['AMOUNT_OF_BEHIND_NODES']}"
                )
                if (
                    self._amount_of_ends_received
                    == self.__config["AMOUNT_OF_BEHIND_NODES"]
                ):
                    self.__send_end_to_forward_queues()

                self.__middleware.ack(delivery_tag)

                return

            # TODO: handle conversion error
            app_id = int(review[0])
            for record in read_by_range(
                "tmp/", int(self.__config["PARTITION_RANGE"]), app_id
            ):
                record_splitted = record[0].split(",", maxsplit=1)
                record_app_id = record_splitted[0]
                if app_id == int(record_app_id):
                    # Get rid of the app_id from the review and append it to the original game record
                    # TODO: QUE NO HAGA UNA LISTA!!
                    # joined_message = [record_app_id, record_info] + review[1:]
                    joined_message = self.__games_columns_to_keep(
                        record_splitted
                    ) + self.__reviews_columns_to_keep(review)

                    if (
                        "Q" in forwarding_queue_name
                    ):  # gotta check this as it could be the last node, then a prefix shouldn't be used
                        self.__middleware.publish(
                            joined_message,
                            forwarding_queue_name,
                        )

                    else:
                        node_id = node_id_to_send_to(
                            "1",
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

        self.__middleware.ack(delivery_tag)

    def __send_end_to_forward_queues(self):
        forwarding_queue_name = self.__config["OUTPUT_QUEUE_NAME"]

        if (
            "Q" in forwarding_queue_name
        ):  # gotta check this as it could be the last node, then a prefix shouldn't be used
            self.__middleware.publish_batch(forwarding_queue_name)
            self.__middleware.send_end(forwarding_queue_name)
            logging.debug(f"Sent end to: {forwarding_queue_name}")
            return

        for i in range(self.__config["AMOUNT_OF_FORWARDING_QUEUES"]):
            self.__middleware.publish_batch(f"{i}_{forwarding_queue_name}")
            self.__middleware.send_end(f"{i}_{forwarding_queue_name}")
            logging.debug(f"Sent end to: {i}_{forwarding_queue_name}")

    def __games_columns_to_keep(self, games_record: list[str]):
        return [games_record[i] for i in self.__config["GAMES_COLUMNS_TO_KEEP"]]

    def __reviews_columns_to_keep(self, reviews_record: list[str]):
        return [reviews_record[i] for i in self.__config["REVIEWS_COLUMNS_TO_KEEP"]]
