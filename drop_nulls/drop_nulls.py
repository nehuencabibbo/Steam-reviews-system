import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import *
from common.protocol.protocol import Protocol
from common.middleware.middleware import Middleware, MiddlewareError
from utils.utils import node_id_to_send_to
from constants import *

import signal
import logging


class DropNulls:
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
        self.r = 0
        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def start(self):
        # Reciving queues
        self._middleware.create_queue(self._config["GAMES_RECIVING_QUEUE_NAME"])
        self._middleware.create_queue(self._config["REVIEWS_RECIVING_QUEUE_NAME"])
        # Forwarding queues
        # Q1
        for i in range(self._config["COUNT_BY_PLATFORM_NODES"]):
            self._middleware.create_queue(f"{i}_{self._config['Q1_PLATFORM']}")

        # Q2
        self._middleware.create_queue(self._config["Q2_GAMES"])

        # Q3, Q4, Q5
        for i in range(3, 6):
            self._middleware.create_queue(self._config[f"Q{i}_GAMES"])
            self._middleware.create_queue(self._config[f"Q{i}_REVIEWS"])

        # Attaching callback functions
        games_callback = self._middleware.__class__.generate_callback(
            self.__handle_games,
        )
        self._middleware.attach_callback(
            self._config["GAMES_RECIVING_QUEUE_NAME"],
            games_callback,
        )

        reviews_callback = self._middleware.__class__.generate_callback(
            self.__handle_reviews,
        )
        self._middleware.attach_callback(
            self._config["REVIEWS_RECIVING_QUEUE_NAME"], reviews_callback
        )

        try:
            self._middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)

    def __handle_end_transmission(
        self, body: List[str], reciving_queue_name: str, message_type: str
    ):
        # Si me llego un END...
        # 1) Me fijo si los la cantidad de ids que hay es igual a
        # la cantidad total de instancias de mi mismo que hay.
        # Si es asi => Envio el END a la proxima cola
        # Si no es asi => Checkeo si mi ID esta en la lista
        #     Si es asi => No agrego nada y reencolo
        #     Si no es asi => Agrego mi id a la lista y reencolo

        peers_that_recived_end = body[2:]
        client_id = body[0]

        if len(peers_that_recived_end) == int(self._config["INSTANCES_OF_MYSELF"]):
            # encoded_message = self._protocol.encode([END_TRANSMISSION_MESSAGE])

            if message_type == GAMES_MESSAGE_TYPE:
                self.__handle_games_end_transmission_by_query(client_id)
            elif message_type == REVIEWS_MESSAGE_TYPE:
                self.__handle_reviews_end_transmission_by_query(client_id)
            else:
                raise Exception(f"Unkown message type: {message_type}")

        else:

            if message_type == GAMES_MESSAGE_TYPE:
                self.__handle_games_last_batch()
            elif message_type == REVIEWS_MESSAGE_TYPE:
                self.__handle_reviews_last_batch()
            else:
                raise Exception(f"Unkown message type: {message_type}")

            message = [client_id, END_TRANSMISSION_MESSAGE]
            if not self._config["NODE_ID"] in peers_that_recived_end:
                peers_that_recived_end.append(self._config["NODE_ID"])

            message += peers_that_recived_end
            self._middleware.publish_message(message, reciving_queue_name)

    def __handle_games_last_batch(self):

        for i in range(self._config["COUNT_BY_PLATFORM_NODES"]):
            queue_name = f"{i}_{self._config['Q1_PLATFORM']}"
            self._middleware.publish_batch(queue_name)

        # Q2, Q3, Q4, Q5
        for i in range(2, 6):
            queue_name = self._config[f"Q{i}_GAMES"]
            self._middleware.publish_batch(queue_name)

    def __handle_games_end_transmission_by_query(self, client_id: str):
        # encoded_message = self._protocol.encode([END_TRANSMISSION_MESSAGE])
        # Mandar el END a cada uno particular
        # Como tengo N nodos de count by platform, a cada uno de ellos le tiene que llegar el end
        # Q1
        for i in range(self._config["COUNT_BY_PLATFORM_NODES"]):
            logging.debug(f"SENDING_TO: {i}_{self._config['Q1_PLATFORM']}")
            # self._middleware.publish(
            #     encoded_message, f"{i}_{self._config['Q1_PLATFORM']}"
            # )
            self._middleware.send_end(
                queue=f"{i}_{self._config['Q1_PLATFORM']}",
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )

        # Q2, Q3, Q4, Q5
        for i in range(2, 6):
            # self._middleware.publish(encoded_message, self._config[f"Q{i}_GAMES"])
            self._middleware.send_end(
                queue=self._config[f"Q{i}_GAMES"],
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )

    def __handle_games(self, delivery_tag: int, body: bytes):
        body = self._middleware.get_rows_from_message(message=body)

        for message in body:
            logging.debug(f"Recived message: {message}")

            # message = [value.strip() for value in message]
            # For how is the filter_columns made, the first END is prefixed with the client_id
            if message[1] == END_TRANSMISSION_MESSAGE:
                logging.debug(f"Recived games END: {message}")
                self.__handle_end_transmission(
                    message,
                    self._config["GAMES_RECIVING_QUEUE_NAME"],
                    GAMES_MESSAGE_TYPE,
                )

                self._middleware.ack(delivery_tag)
                return

            if NULL_FIELD_VALUE in message:
                logging.debug("Dropped prev value")
                continue
                # self._middleware.ack(delivery_tag)
                # return

            client_id = message[0]

            # Q1 Platform: plataform
            for platform, platform_index in PLATFORMS.items():
                platform_supported = message[
                    platform_index
                ]  # True if supported else False

                # TODO: Should this be handeled in a different node?
                if platform_supported.lower() == "true":
                    node_id = node_id_to_send_to(
                        client_id, platform, self._config["COUNT_BY_PLATFORM_NODES"]
                    )
                    # encoded_message = self._protocol.encode([platform])
                    self._middleware.publish(
                        [client_id, platform],
                        f"{node_id}_{self._config['Q1_PLATFORM']}",
                    )

            # Q2 Games: app_id, name, release date, genre, avg playtime forever
            # encoded_message = self._protocol.encode(
            #     [
            #         message[GAMES_APP_ID],
            #         message[GAMES_NAME],
            #         message[GAMES_RELEASE_DATE],
            #         message[GAMES_AVG_PLAYTIME_FOREVER],
            #         message[GAMES_GENRE],
            #     ]
            # )
            self._middleware.publish(
                [
                    client_id,
                    message[GAMES_APP_ID],
                    message[GAMES_NAME],
                    message[GAMES_RELEASE_DATE],
                    message[GAMES_AVG_PLAYTIME_FOREVER],
                    message[GAMES_GENRE],
                ],
                self._config["Q2_GAMES"],
            )

            # Q3, Q4, Q5 Games: app_id, name, genre
            # encoded_message = self._protocol.encode(
            #     [message[GAMES_APP_ID], message[GAMES_NAME], message[GAMES_GENRE]]
            # )

            for i in range(3, 6):
                self._middleware.publish(
                    [
                        client_id,
                        message[GAMES_APP_ID],
                        message[GAMES_NAME],
                        message[GAMES_GENRE],
                    ],
                    self._config[f"Q{i}_GAMES"],
                )

        self._middleware.ack(delivery_tag)

    def __handle_reviews_end_transmission_by_query(self, client_id: str):
        for i in range(3, 6):
            # encoded_message = self._protocol.encode([END_TRANSMISSION_MESSAGE])
            self._middleware.send_end(
                self._config[f"Q{i}_REVIEWS"],
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )

    def __handle_reviews_last_batch(self):

        for i in range(3, 6):
            queue_name = self._config[f"Q{i}_REVIEWS"]
            self._middleware.publish_batch(queue_name)

    def __handle_reviews(self, delivery_tag: int, body: bytes):
        body = self._middleware.get_rows_from_message(message=body)
        for message in body:
            # message = [value.strip() for value in message]

            if message[1] == END_TRANSMISSION_MESSAGE:
                logging.debug(f"Recived reviews END: {message}")
                self.__handle_end_transmission(
                    message,
                    self._config["REVIEWS_RECIVING_QUEUE_NAME"],
                    REVIEWS_MESSAGE_TYPE,
                )
                self._middleware.ack(delivery_tag)

                return

            logging.debug(f"Recived review: {message}")
            if NULL_FIELD_VALUE in message:
                logging.debug("NULL was found in recived review, dropping it")
                continue

            client_id = message[0]
            # Q3, Q5 Reviews: app_id, review_score
            for i in ["3", "5"]:
                # encoded_message = self._protocol.encode(
                #     [message[REVIEW_APP_ID], message[REVIEW_SCORE]]
                # )
                self._middleware.publish(
                    [client_id, message[REVIEW_APP_ID], message[REVIEW_SCORE]],
                    self._config[f"Q{i}_REVIEWS"],
                )

            # Q4 Reviews: app_id, review_text, review_score
            # encoded_message = self._protocol.encode(
            #     [message[REVIEW_APP_ID], message[REVIEW_TEXT], message[REVIEW_SCORE]]
            # )
            self.r += 1
            logging.debug(f"amount of reviews sent to q4: {self.r}")
            self._middleware.publish(
                [
                    client_id,
                    message[REVIEW_APP_ID],
                    message[REVIEW_TEXT],
                    message[REVIEW_SCORE],
                ],
                self._config[f"Q4_REVIEWS"],
            )

        self._middleware.ack(delivery_tag)

    def __signal_handler(self, sig, frame):
        logging.debug(
            f"[NULL DROP {self._config['NODE_ID']}] Gracefully shutting down..."
        )
        self._got_sigterm = True
        self._middleware.shutdown()
