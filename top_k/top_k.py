import logging
import signal
from middleware.middleware import Middleware
from common.storage.storage import read_by_range, write_by_range
from common.protocol.protocol import Protocol

END_TRANSMISSION_MESSAGE = "END"


class TopK:
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
        self.__middleware.create_queue(self.__config["INPUT_TOP_K_QUEUE_NAME"])
        self.__middleware.create_queue(self.__config["OUTPUT_TOP_K_QUEUE_NAME"])

        # # callback, inputq, outputq
        games_callback = self.__middleware.generate_callback(
            self.__callback,
            self.__config["INPUT_TOP_K_QUEUE_NAME"],
            self.__config["OUTPUT_TOP_K_QUEUE_NAME"],
        )

        self.__middleware.attach_callback(
            self.__config["INPUT_TOP_K_QUEUE_NAME"], games_callback
        )

        self.__middleware.start_consuming()

    def __callback(self, delivery_tag, body, message_type, forwarding_queue_name):
        logging.debug(f"[INPUT GAMES] received: {body}")

        body = self.__protocol.decode(body)
        body = [value.strip() for value in body]

        if len(body) == 1 and body[0] == END_TRANSMISSION_MESSAGE:
            logging.debug("END of games received")
            # TODO: Aca manda el topk
            encoded_message = self.__protocol.encode([END_TRANSMISSION_MESSAGE])
            self.__middleware.publish(encoded_message, forwarding_queue_name, "")
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

    def __get_top_k(self):
        pass
        # records = read_by_range
