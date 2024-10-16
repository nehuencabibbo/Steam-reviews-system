import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import *
from common.middleware.middleware import Middleware, MiddlewareError
from common.protocol.protocol import Protocol
import signal
import logging

GAMES_MESSAGE_TYPE = "games"
REVIEWS_MESSAGE_TYPE = "reviews"
END_TRANSMISSION_MESSAGE = "END"


class FilterColumns:
    def __init__(
        self,
        protocol: Protocol,
        middleware: Middleware,
        config: Dict[str, Union[str, int]],
    ):
        self._protocol = protocol
        self._middleware = middleware
        self._config = config
        self._got_sigterm = False

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def start(self):
        # Queues that the client uses to send data
        self._middleware.create_queue(self._config["CLIENT_GAMES_QUEUE_NAME"])
        self._middleware.create_queue(self._config["CLIENT_REVIEWS_QUEUE_NAME"])
        # Queues that filter columns uses to send data to null drop
        self._middleware.create_queue(self._config["NULL_DROP_GAMES_QUEUE_NAME"])
        self._middleware.create_queue(self._config["NULL_DROP_REVIEWS_QUEUE_NAME"])

        games_callback = self._middleware.__class__.generate_callback(
            self.__handle_message,
            GAMES_MESSAGE_TYPE,
            self._config["NULL_DROP_GAMES_QUEUE_NAME"],
        )
        self._middleware.attach_callback(
            self._config["CLIENT_GAMES_QUEUE_NAME"], games_callback
        )

        reviews_callback = self._middleware.__class__.generate_callback(
            self.__handle_message,
            REVIEWS_MESSAGE_TYPE,
            self._config["NULL_DROP_REVIEWS_QUEUE_NAME"],
        )
        self._middleware.attach_callback(
            self._config["CLIENT_REVIEWS_QUEUE_NAME"], reviews_callback
        )

        self._middleware.turn_fair_dispatch()
        try:
            self._middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)

    def __handle_end_transmission(
        self, body: List[str], reciving_queue_name: str, forwarding_queue_name: str
    ):
        # Si me llego un END...
        # 1) Me fijo si los la cantidad de ids que hay es igual a
        # la cantidad total de instancias de mi mismo que hay.
        # Si es asi => Envio el END a la proxima cola
        # Si no es asi => Checkeo si mi ID esta en la lista
        #     Si es asi => No agrego nada y reencolo
        #     Si no es asi => Agrego mi id a la lista y reencolo
        peers_that_recived_end = body[1:]
        if len(peers_that_recived_end) == int(self._config["INSTANCES_OF_MYSELF"]):
            logging.debug("Sending real END")
            self._middleware.send_end(queue=forwarding_queue_name)
        else:
            self._middleware.publish_batch(forwarding_queue_name)
            message = [END_TRANSMISSION_MESSAGE]
            if not self._config["NODE_ID"] in peers_that_recived_end:
                peers_that_recived_end.append(self._config["NODE_ID"])

            message += peers_that_recived_end
            self._middleware.publish_message(message, reciving_queue_name)

    def __handle_message(
        self,
        delivery_tag: int,
        body: bytes,
        message_type: str,
        forwarding_queue_name: str,
    ):
        body = self._middleware.get_rows_from_message(body)
        for message in body:
            logging.debug(f"Recived message: {message}")

            # message = [value.strip() for value in message]

            if message[0] == END_TRANSMISSION_MESSAGE:
                logging.debug(f"Recived END from {message_type}: {message}")
                if message_type == GAMES_MESSAGE_TYPE:
                    self.__handle_end_transmission(
                        message,
                        self._config["CLIENT_GAMES_QUEUE_NAME"],
                        self._config["NULL_DROP_GAMES_QUEUE_NAME"],
                    )
                elif message_type == REVIEWS_MESSAGE_TYPE:
                    self.__handle_end_transmission(
                        message,
                        self._config["CLIENT_REVIEWS_QUEUE_NAME"],
                        self._config["NULL_DROP_REVIEWS_QUEUE_NAME"],
                    )
                else:
                    raise Exception(f"Unkown message type: {message_type}")

                self._middleware.ack(delivery_tag)

                return

            logging.debug(
                f"[FILTER COLUMNS {self._config['NODE_ID']}] Recived {message_type}: {message}"
            )

            columns_to_keep = []
            if message_type == GAMES_MESSAGE_TYPE:
                columns_to_keep = self._config["GAMES_COLUMNS_TO_KEEP"]
            elif message_type == REVIEWS_MESSAGE_TYPE:
                columns_to_keep = self._config["REVIEWS_COLUMNS_TO_KEEP"]
            else:
                # Message type was not set properly, unrecoverable error
                raise Exception(f"[ERROR] Unkown message type {message_type}")

            filtered_body = self.__filter_columns(columns_to_keep, message)

            logging.debug(
                f"[FILTER COLUMNS {self._config['NODE_ID']}] Sending {message_type}: {message}"
            )

            self._middleware.publish(filtered_body, forwarding_queue_name, "")

        self._middleware.ack(delivery_tag)

    def __filter_columns(self, columns_to_keep: List[int], data: List[str]):
        return [data[i] for i in columns_to_keep]

    def __signal_handler(self, sig, frame):
        logging.debug(
            f"[FILTER COLUMNS {self._config['NODE_ID']}] Gracefully shutting down..."
        )
        self._got_sigterm = True
        self._middleware.shutdown()
