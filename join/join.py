import logging
import signal
from common.middleware.middleware import Middleware
from common.storage.storage import read_by_range, write_by_range
from common.protocol.protocol import Protocol
from utils.utils import node_id_to_send_to

END_TRANSMISSION_MESSAGE = "END"


class Join:
    def __init__(
        self, protocol: Protocol, middleware: Middleware, config: dict[str, str]
    ):
        self.__protocol = protocol
        self.__middleware = middleware
        self.__config = config
        self._amount_of_ends_received = 0

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def __signal_handler(self, sig, frame):
        logging.debug(f"Gracefully shutting down...")
        self.__middleware.shutdown()

    def start(self):
        self.__middleware.create_queue(self.__config["INPUT_GAMES_QUEUE_NAME"])
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

        self.__middleware.start_consuming()

    def __games_callback(self, delivery_tag, body, message_type, forwarding_queue_name):
        # logging.debug(f"[INPUT GAMES] received: {body}")

        # body = body.decode("utf-8").split(",")
        # body = self.__protocol.decode(body)
        # body = [value.strip() for value in body]
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
        # logging.debug(f"[INPUT REVIEWS] received: {body}")

        # message = body.decode("utf-8")
        # body = self.__protocol.decode(body)

        # body = [value.strip() for value in body]

        body = self.__middleware.get_rows_from_message(body)
        for review in body:
            logging.debug(f"Recived review: {review}")

            if len(review) == 1 and review[0] == END_TRANSMISSION_MESSAGE:
                #send rest of batch if there is any
                self.__middleware.publish_batch(forwarding_queue_name)

                logging.debug("END of reviews received")

                self._amount_of_ends_received += 1
                logging.debug(
                    f"Amount of ends received up to now: {self._amount_of_ends_received} | Expecting: {self.__config['AMOUNT_OF_BEHIND_NODES']}"
                )
                if self._amount_of_ends_received == self.__config["AMOUNT_OF_BEHIND_NODES"]:
                    self.__middleware.send_end(forwarding_queue_name)

                    # encoded_message = self.__protocol.encode([END_TRANSMISSION_MESSAGE])
                    # self.__middleware.publish(encoded_message, forwarding_queue_name, "")

                self.__middleware.ack(delivery_tag)

                return

            # TODO: handle conversion error
            app_id = int(review[0])
            for record in read_by_range(
                "tmp/", int(self.__config["PARTITION_RANGE"]), app_id
            ):
                record_app_id, record_info = record[0].split(",", maxsplit=1)
                if app_id == int(record_app_id):
                    # Get rid of the app_id from the review and append it to the original game record
                    joined_message = [record_info, review[1]]

                    # encoded_message = self.__protocol.encode([joined_message])
                    self.__middleware.publish(joined_message, forwarding_queue_name, "")

        self.__middleware.ack(delivery_tag)

    def __send_end_to_forward_queues(self):
        encoded_message = self.__protocol.encode([END_TRANSMISSION_MESSAGE])
        forwarding_queue_name = self.__config["OUTPUT_QUEUE_NAME"]

        if (
            "Q" in forwarding_queue_name
        ):  # gotta check this as it could be the last node, then a prefix shouldn't be used
            self.__middleware.publish(encoded_message, forwarding_queue_name, "")

        for i in range(self.__config["AMOUNT_OF_FORWARDING_QUEUES"]):
            self.__middleware.publish(
                encoded_message, f"{i}_{forwarding_queue_name}", ""
            )
            logging.debug(f"Sent end to: {i}_{forwarding_queue_name}")


# TODO: make sigterm handle
