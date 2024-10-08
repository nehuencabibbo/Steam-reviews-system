import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import *
from common.protocol.protocol import Protocol
from middleware.middleware import Middleware
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
        # for i in range(3, 6):
        for i in range(3, 5):
            self._middleware.create_queue(self._config[f"Q{i}_GAMES"])
            self._middleware.create_queue(self._config[f"Q{i}_REVIEWS"])

        # Attaching callback functions
        games_callback = self._middleware.__class__.generate_callback(
            self.__handle_games,
        )
        self._middleware.attach_callback(
            self._config["GAMES_RECIVING_QUEUE_NAME"], games_callback
        )

        reviews_callback = self._middleware.__class__.generate_callback(
            self.__handle_reviews,
        )
        self._middleware.attach_callback(
            self._config["REVIEWS_RECIVING_QUEUE_NAME"], reviews_callback
        )

        self._middleware.start_consuming()

    def __handle_end_transmission(self, body: List[str], reciving_queue_name: str, message_type: str):
        # Si me llego un END...
        # 1) Me fijo si los la cantidad de ids que hay es igual a 
        # la cantidad total de instancias de mi mismo que hay.
        # Si es asi => Envio el END a la proxima cola
        # Si no es asi => Checkeo si mi ID esta en la lista
        #     Si es asi => No agrego nada y reencolo
        #     Si no es asi => Agrego mi id a la lista y reencolo
        peers_that_recived_end = body[1:]
        if len(peers_that_recived_end) == int(self._config["INSTANCES_OF_MYSELF"]):
            encoded_message = self._protocol.encode([END_TRANSMISSION_MESSAGE])
            
            if message_type == GAMES_MESSAGE_TYPE:
                self.__handle_games_end_transmission_by_query()
            elif message_type == REVIEWS_MESSAGE_TYPE:
                self.__handle_reviews_end_transmission_by_query()
            else:
                raise Exception(f"Unkown message type: {message_type}")

        else: 
            message = [END_TRANSMISSION_MESSAGE]
            if not self._config["NODE_ID"] in peers_that_recived_end:
                peers_that_recived_end.append(self._config["NODE_ID"])
                
            message += peers_that_recived_end
            encoded_message = self._protocol.encode(message)
            self._middleware.publish(encoded_message, reciving_queue_name, "")

    def __handle_games_end_transmission_by_query(self):
        encoded_message = self._protocol.encode([END_TRANSMISSION_MESSAGE])
        # Mandar el END a cada uno particular 
        # Como tengo N nodos de count by platform, a cada uno de ellos le tiene que llegar el end
        # Q1
        for i in range(self._config["COUNT_BY_PLATFORM_NODES"]):
            logging.debug(f"SENDING_TO: {i}_{self._config['Q1_PLATFORM']}")
            self._middleware.publish(
                encoded_message, 
                f"{i}_{self._config['Q1_PLATFORM']}"
            )

        # Q2, Q3, Q4, Q5
        for i in range(2, 5): # TODO: No se manda a la Q5 por ahora
            self._middleware.publish(encoded_message, self._config[f"Q{i}_GAMES"])

    def __handle_games(self, delivery_tag: int, body: bytes):
        body = self._protocol.decode(body)
        body = [value.strip() for value in body]
    
        if body[0] == END_TRANSMISSION_MESSAGE:
            logging.debug(f"Recived games END: {body}")
            self.__handle_end_transmission(
                body, 
                self._config["GAMES_RECIVING_QUEUE_NAME"],
                GAMES_MESSAGE_TYPE
            )

            self._middleware.ack(delivery_tag)

            return

        logging.debug(f"Recived game: {body}")
        if NULL_FIELD_VALUE in body:
            self._middleware.ack(delivery_tag)
            return

        # Q1 Platform: plataform
        for platform, platform_index in PLATFORMS.items():
            platform_supported = body[platform_index]  # True if supported else False

            # TODO: Should this be handeled in a different node?
            if platform_supported.lower() == "true":
                node_id = node_id_to_send_to(
                    "1", 
                    platform, 
                    self._config["COUNT_BY_PLATFORM_NODES"]
                )
                encoded_message = self._protocol.encode([platform])
                self._middleware.publish(
                    encoded_message, f"{node_id}_{self._config['Q1_PLATFORM']}"
                )

        # Q2 Games: app_id, name, release date, genre, avg playtime forever
        encoded_message = self._protocol.encode(
            [
                body[GAMES_APP_ID],
                body[GAMES_NAME],
                body[GAMES_RELEASE_DATE],
                body[GAMES_AVG_PLAYTIME_FOREVER],
                body[GAMES_GENRE],
            ]
        )
        self._middleware.publish(encoded_message, self._config["Q2_GAMES"])

        # Q3, Q4, Q5 Games: app_id, name, genre
        encoded_message = self._protocol.encode(
            [body[GAMES_APP_ID], body[GAMES_NAME], body[GAMES_GENRE]]
        )

        for i in range(3, 6):
            self._middleware.publish(encoded_message, self._config[f"Q{i}_GAMES"])

        self._middleware.ack(delivery_tag)

    def __handle_reviews_end_transmission_by_query(self):
        for i in range(3, 6):
            encoded_message = self._protocol.encode([END_TRANSMISSION_MESSAGE])
            self._middleware.publish(encoded_message, self._config[f"Q{i}_REVIEWS"])

    def __handle_reviews(self, delivery_tag: int, body: bytes):
        body = self._protocol.decode(body)
        body = [value.strip() for value in body]
        if body[0] == END_TRANSMISSION_MESSAGE:
            logging.debug(f"Recived reviews END: {body}")
            self.__handle_end_transmission(
                body, 
                self._config["REVIEWS_RECIVING_QUEUE_NAME"],
                REVIEWS_MESSAGE_TYPE
            )
            self._middleware.ack(delivery_tag)

            return

        logging.debug(f"[NULL DROP {self._config['NODE_ID']}] Recived review: {body}")
        # Q3, Q5 Reviews: app_id, review_score
        # for i in ["3", "5"]:
        for i in ["3"]:
            encoded_message = self._protocol.encode(
                [body[REVIEW_APP_ID], body[REVIEW_SCORE]]
            )
            self._middleware.publish(encoded_message, self._config[f"Q{i}_REVIEWS"])

        # Q4 Reviews: app_id, review_text, review_score
        encoded_message = self._protocol.encode(
            [body[REVIEW_APP_ID], body[REVIEW_TEXT], body[REVIEW_SCORE]]
        )
        self._middleware.publish(encoded_message, self._config[f"Q4_REVIEWS"])

        self._middleware.ack(delivery_tag)


    def __signal_handler(self, sig, frame):
        logging.debug(
            f"[NULL DROP {self._config['NODE_ID']}] Gracefully shutting down..."
        )
        self._middleware.shutdown()
