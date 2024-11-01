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
        self._got_sigterm = False

        self._client_games_queue_name = config["CLIENT_GAMES_QUEUE_NAME"]
        self._client_reviews_queue_name = config["CLIENT_REVIEWS_QUEUE_NAME"]
        self._null_drop_games_queue_name = config["NULL_DROP_GAMES_QUEUE_NAME"]
        self._null_drop_reviews_queue_name = config["NULL_DROP_REVIEWS_QUEUE_NAME"]
        self._instances_of_myself = config["INSTANCES_OF_MYSELF"]
        self._node_id = config["NODE_ID"]
        self._reviews_columns_to_keep = config["REVIEWS_COLUMNS_TO_KEEP"]
        self._game_columns_to_keep = config["GAMES_COLUMNS_TO_KEEP"]

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def start(self):
        # Queues that the client uses to send data
        # anonymous_queue_name = self._middleware.create_anonymous_queue()
        # self._middleware.bind_queue_to_exchange(
        #     exchange_name=self._config["NEW_CLIENTS_EXCHANGE_NAME"],
        #     queue_name=anonymous_queue_name,
        # )

        # new_clients_callback = self._middleware.__class__.generate_callback(
        #     self.__handle_new_clients
        # )
        # self._middleware.attach_callback(anonymous_queue_name, new_clients_callback)

        self._middleware.create_queue(self._client_games_queue_name)
        self._middleware.create_queue(self._client_reviews_queue_name)
        # Queues that filter columns uses to send data to null drop
        self._middleware.create_queue(self._null_drop_games_queue_name)
        self._middleware.create_queue(self._null_drop_reviews_queue_name)

        games_callback = self._middleware.__class__.generate_callback(
            self.__handle_games,
            self._client_games_queue_name,
            self._null_drop_games_queue_name,
        )
        self._middleware.attach_callback(self._client_games_queue_name, games_callback)

        reviews_callback = self._middleware.__class__.generate_callback(
            self.__handle_reviews,
            self._client_reviews_queue_name,
            self._null_drop_reviews_queue_name,
        )
        self._middleware.attach_callback(
            self._client_reviews_queue_name,
            reviews_callback,
        )

        try:
            self._middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self._middleware.shutdown()

    def __handle_end_transmission(
        self,
        body: List[str],
        reciving_queue_name: str,
        forwarding_queue_name: str,
        client_id,
    ):
        # Si me llego un END...
        # 1) Me fijo si los la cantidad de ids que hay es igual a
        # la cantidad total de instancias de mi mismo que hay.
        # Si es asi => Envio el END a la proxima cola
        # Si no es asi => Checkeo si mi ID esta en la lista
        #     Si es asi => No agrego nada y reencolo
        #     Si no es asi => Agrego mi id a la lista y reencolo

        # Have to check if it's a client end, in which case only "END" is received, otherwise, the client ID comes
        # first
        # peers_that_recived_end = body[1:] if len(body) == 1 else body[2:]
        peers_that_recived_end = body[1:]

        if len(peers_that_recived_end) == int(self._instances_of_myself):
            logging.debug("Sending real END")
            self._middleware.send_end(
                queue=forwarding_queue_name,
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )
        else:
            self._middleware.publish_batch(forwarding_queue_name)
            # TODO: cambiar esto, por que se crea otra lista??? reusar la de body y fulbo
            # Si lo dejamos asi, un set para mejor eficiencia en vez de una lista
            message = [client_id, END_TRANSMISSION_MESSAGE]
            if not self._node_id in peers_that_recived_end:
                peers_that_recived_end.append(self._node_id)

            message += peers_that_recived_end
            self._middleware.publish_message(message, reciving_queue_name)
            logging.debug(f"Publishing: {message} in {reciving_queue_name}")

    def __handle_games(
        self,
        delivery_tag: int,
        body: bytes,
        input_queue_name: str,
        forwarding_queue_name: str,
    ):
        body = self._middleware.get_rows_from_message(body)
        logging.debug(f"[FILTER COLUMNS {self._node_id}] Recived games body: {body}")
        if len(body) > 1:
            client_id = body.pop(0)[0]  # Get client_id,
        else:
            client_id = body[0].pop(0)  # END message scenario

        logging.debug(f"client_id: {client_id}")
        for message in body:
            # Have to check both, the END from the client, and the consensus END, which has the client id as
            # prefix
            if message[0] == END_TRANSMISSION_MESSAGE:
                logging.debug(f"Recived END of games: {message}")
                self.__handle_end_transmission(
                    message,
                    input_queue_name,
                    self._null_drop_games_queue_name,
                    client_id=client_id,
                )

                self._middleware.ack(delivery_tag)

                return

            logging.debug(f"[FILTER COLUMNS {self._node_id}] Recived game: {message}")

            columns_to_keep = []
            columns_to_keep = self._game_columns_to_keep

            filtered_body = self.__filter_columns(
                columns_to_keep, message, client_id=client_id
            )

            logging.debug(f"[FILTER COLUMNS {self._node_id}] Sending games: {message}")

            self._middleware.publish(filtered_body, forwarding_queue_name, "")

        self._middleware.ack(delivery_tag)

    def __handle_reviews(
        self,
        delivery_tag: int,
        body: bytes,
        input_queue_name: str,
        forwarding_queue_name: str,
    ):
        body = self._middleware.get_rows_from_message(body)

        if len(body) > 1:
            client_id = body.pop(0)[0]  # Get client_id,
        else:
            client_id = body[0].pop(0)  # END message scenario

        logging.debug(f"client_id: {client_id}")

        for message in body:

            # Have to check both, the END from the client, and the consensus END, which has the client id as
            # prefix
            if message[0] == END_TRANSMISSION_MESSAGE:
                logging.debug(f"Recived END of reviews: {message}")

                self.__handle_end_transmission(
                    message,
                    input_queue_name,
                    self._null_drop_reviews_queue_name,
                    client_id=client_id,
                )
                self._middleware.ack(delivery_tag)

                return

            logging.debug(f"[FILTER COLUMNS {self._node_id}] Recived review: {message}")

            columns_to_keep = []
            columns_to_keep = self._reviews_columns_to_keep

            filtered_body = self.__filter_columns(
                columns_to_keep, message, client_id=client_id
            )

            logging.debug(f"[FILTER COLUMNS {self._node_id}] Sending review: {message}")

            self._middleware.publish(filtered_body, forwarding_queue_name, "")

        self._middleware.ack(delivery_tag)

    def __filter_columns(self, columns_to_keep: List[int], data: List[str], client_id):
        filtered_data = [client_id]
        for i in columns_to_keep:
            filtered_data.append(data[i])
        return filtered_data

    def __signal_handler(self, sig, frame):
        logging.debug(f"[FILTER COLUMNS {self._node_id}] Gracefully shutting down...")
        self._got_sigterm = True
        # self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
