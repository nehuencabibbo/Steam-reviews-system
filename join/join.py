import logging
import signal
from middleware.middleware import Middleware
from common.storage.storage import read_by_range, write_by_range
from common.protocol.protocol import Protocol

END_TRANSMISSION_MESSAGE = "END"


class Join:
    def __init__(
        self, protocol: Protocol, middleware: Middleware, config: dict[str, str]
    ):
        self.__protocol = protocol
        self.__middleware = middleware
        self.__config = config

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def __signal_handler(self, sig, frame):
        logging.debug(f"[JOIN {self._config['NODE_ID']}] Gracefully shutting down...")
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
        logging.debug(f"[INPUT GAMES] received: {body}")

        # body = body.decode("utf-8").split(",")
        body = self.__protocol.decode(body)
        body = [value.strip() for value in body]

        if len(body) == 1 and body[0] == END_TRANSMISSION_MESSAGE:
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
                "tmp/", int(self.__config["PARTITION_RANGE"]), ",".join(body)
            )
        except ValueError as e:
            logging.error(
                f"An error has occurred. {e}",
            )

        self.__middleware.ack(delivery_tag)

    def __reviews_callback(
        self, delivery_tag, body, message_type, forwarding_queue_name
    ):
        logging.debug(f"[INPUT REVIEWS] received: {body}")

        # message = body.decode("utf-8")
        message = self.__protocol.decode(body)[0]

        body = [value.strip() for value in message.split(",")]

        if len(body) == 1 and body[0] == END_TRANSMISSION_MESSAGE:
            logging.debug("END of reviews received")

            encoded_message = self.__protocol.encode([END_TRANSMISSION_MESSAGE])
            self.__middleware.publish(encoded_message, forwarding_queue_name, "")

            self.__middleware.ack(delivery_tag)

            return

        # TODO: handle conversion error
        app_id = int(body[0])
        for record in read_by_range(
            "tmp/", int(self.__config["PARTITION_RANGE"]), app_id
        ):
            record_app_id = int(record[0].split(",", maxsplit=1)[0])
            if app_id == record_app_id:
                # Get rid of the app_id from the review and append it to the original game record
                joined_message = record[0] + "," + message.split(",", maxsplit=1)[1]
                encoded_message = self.__protocol.encode([joined_message])
                self.__middleware.publish(encoded_message, forwarding_queue_name, "")

        self.__middleware.ack(delivery_tag)


# TODO: make sigterm handle