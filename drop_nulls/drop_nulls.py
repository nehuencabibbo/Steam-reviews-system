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
        self._got_sigterm = False
        self.r = 0

        # Directly assign each config value to an attribute
        self.games_receiving_queue_name = config["GAMES_RECIVING_QUEUE_NAME"]
        self.reviews_receiving_queue_name = config["REVIEWS_RECIVING_QUEUE_NAME"]
        self.count_by_platform_nodes = config["COUNT_BY_PLATFORM_NODES"]
        self.q1_platform = config["Q1_PLATFORM"]
        self.q2_games = config["Q2_GAMES"]
        self.q3_games = config["Q3_GAMES"]
        self.q4_games = config["Q4_GAMES"]
        self.q5_games = config["Q5_GAMES"]
        self.q3_reviews = config["Q3_REVIEWS"]
        self.q4_reviews = config["Q4_REVIEWS"]
        self.q5_reviews = config["Q5_REVIEWS"]
        self.node_id = config["NODE_ID"]
        self.instances_of_myself = int(config["INSTANCES_OF_MYSELF"])

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def start(self):
        # Receiving queues
        self._middleware.create_queue(self.games_receiving_queue_name)
        self._middleware.create_queue(self.reviews_receiving_queue_name)

        # Forwarding queues
        for i in range(self.count_by_platform_nodes):
            self._middleware.create_queue(f"{i}_{self.q1_platform}")

        # Q2, Q3, Q4, Q5
        self._middleware.create_queue(self.q2_games)
        for queue in [self.q3_games, self.q4_games, self.q5_games]:
            self._middleware.create_queue(queue)
        for queue in [self.q3_reviews, self.q4_reviews, self.q5_reviews]:
            self._middleware.create_queue(queue)

        # Attaching callback functions
        games_callback = self._middleware.__class__.generate_callback(
            self.__handle_games,
        )
        self._middleware.attach_callback(
            self.games_receiving_queue_name,
            games_callback,
        )

        reviews_callback = self._middleware.__class__.generate_callback(
            self.__handle_reviews,
        )
        self._middleware.attach_callback(
            self.reviews_receiving_queue_name, reviews_callback
        )

        try:
            self._middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self._middleware.shutdown()

    def __handle_end_transmission(
        self, body: List[str], reciving_queue_name: str, message_type: str
    ):
        peers_that_received_end = body[2:]
        client_id = body[0]

        if len(peers_that_received_end) == self.instances_of_myself:
            logging.debug(f"Sending real END. {message_type}")
            if message_type == GAMES_MESSAGE_TYPE:
                self.__handle_games_end_transmission_by_query(client_id)
            elif message_type == REVIEWS_MESSAGE_TYPE:
                self.__handle_reviews_end_transmission_by_query(client_id)
            else:
                raise Exception(f"Unknown message type: {message_type}")

        else:
            if message_type == GAMES_MESSAGE_TYPE:
                self.__handle_games_last_batch()
            elif message_type == REVIEWS_MESSAGE_TYPE:
                self.__handle_reviews_last_batch()
            else:
                raise Exception(f"Unknown message type: {message_type}")

            message = [client_id, END_TRANSMISSION_MESSAGE]
            if self.node_id not in peers_that_received_end:
                peers_that_received_end.append(self.node_id)

            message += peers_that_received_end
            self._middleware.publish_message(message, reciving_queue_name)

    def __handle_games_last_batch(self):
        for i in range(self.count_by_platform_nodes):
            queue_name = f"{i}_{self.q1_platform}"
            self._middleware.publish_batch(queue_name)

        for queue in [self.q2_games, self.q3_games, self.q4_games, self.q5_games]:
            self._middleware.publish_batch(queue)

    def __handle_games_end_transmission_by_query(self, client_id: str):
        for i in range(self.count_by_platform_nodes):
            logging.debug(f"SENDING_TO: {i}_{self.q1_platform}")
            self._middleware.send_end(
                queue=f"{i}_{self.q1_platform}",
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )

        for queue in [self.q2_games, self.q3_games, self.q4_games, self.q5_games]:
            self._middleware.send_end(
                queue=queue,
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )

    def __handle_games(self, delivery_tag: int, body: bytes):
        body = self._middleware.get_rows_from_message(message=body)

        for message in body:
            logging.debug(f"Received message: {message}")

            if message[1] == END_TRANSMISSION_MESSAGE:
                logging.debug(f"Received games END: {message}")
                self.__handle_end_transmission(
                    message,
                    self.games_receiving_queue_name,
                    GAMES_MESSAGE_TYPE,
                )

                self._middleware.ack(delivery_tag)
                return

            if NULL_FIELD_VALUE in message:
                logging.debug("Dropped prev value")
                continue

            client_id = message[GAMES_SESSION_ID]
            msg_id = message[GAMES_MSG_ID]
            for platform, platform_index in PLATFORMS.items():
                platform_supported = message[platform_index]
                if platform_supported.lower() == "true":
                    node_id = node_id_to_send_to(
                        client_id, platform, self.count_by_platform_nodes
                    )
                    message_to_send = [client_id, msg_id, platform]
                    self._middleware.publish(
                        message_to_send,
                        f"{node_id}_{self.q1_platform}",
                    )

            self._middleware.publish(
                [
                    client_id,
                    message[GAMES_MSG_ID],
                    message[GAMES_APP_ID],
                    message[GAMES_NAME],
                    message[GAMES_RELEASE_DATE],
                    message[GAMES_AVG_PLAYTIME_FOREVER],
                    message[GAMES_GENRE],
                ],
                self.q2_games,
            )

            for queue in [self.q3_games, self.q4_games, self.q5_games]:
                self._middleware.publish(
                    [
                        client_id,
                        message[GAMES_MSG_ID],
                        message[GAMES_APP_ID],
                        message[GAMES_NAME],
                        message[GAMES_GENRE],
                    ],
                    queue,
                )

        self._middleware.ack(delivery_tag)

    def __handle_reviews_end_transmission_by_query(self, client_id: str):
        for queue in [self.q3_reviews, self.q5_reviews, self.q4_reviews]:
            self._middleware.send_end(
                queue=queue,
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )

    def __handle_reviews_last_batch(self):
        for queue in [self.q3_reviews, self.q5_reviews, self.q4_reviews]:
            self._middleware.publish_batch(queue)

    def __handle_reviews(self, delivery_tag: int, body: bytes):
        body = self._middleware.get_rows_from_message(message=body)
        for message in body:
            if message[1] == END_TRANSMISSION_MESSAGE:
                logging.debug(f"Received reviews END: {message}")
                self.__handle_end_transmission(
                    message,
                    self.reviews_receiving_queue_name,
                    REVIEWS_MESSAGE_TYPE,
                )
                self._middleware.ack(delivery_tag)
                return

            logging.debug(f"Received review: {message}")
            if NULL_FIELD_VALUE in message:
                logging.debug("NULL was found in received review, dropping it")
                continue

            client_id = message[0]
            for queue in [self.q3_reviews, self.q5_reviews]:
                self._middleware.publish(
                    [
                        client_id, 
                        message[REVIEW_MSG_ID],
                        message[REVIEW_APP_ID], 
                        message[REVIEW_SCORE]
                    ],
                    queue,
                )

            self.r += 1
            self._middleware.publish(
                [
                    client_id,
                    message[REVIEW_MSG_ID],
                    message[REVIEW_APP_ID],
                    message[REVIEW_TEXT],
                    message[REVIEW_SCORE],
                ],
                self.q4_reviews,
            )

        self._middleware.ack(delivery_tag)

    def __signal_handler(self, sig, frame):
        logging.debug(f"[NULL DROP {self.node_id}] Gracefully shutting down...")
        self._got_sigterm = True
        #self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
