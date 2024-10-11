import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import *
from common.middleware.middleware import Middleware, MiddlewareError
from common.protocol.protocol import Protocol
import signal
import logging

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

        self._forwarding_queue = self._config["FORWARDING_QUEUE"]
        self._reciving_queue = self._config["RECIVING_QUEUE"]
        self._node_id = self._config["NODE_ID"]
        self._instances_of_myself = self._config["INSTANCES_OF_MYSELF"]
        self._columns_to_keep = self._config["COLUMNS_TO_KEEP"]

        self._got_sigterm = False 

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def start(self):
        # Queues that the client uses to send data
        self._middleware.create_queue(self._forwarding_queue)

        # Queues that filter columns uses to send data to null drop
        self._middleware.create_queue(self._reciving_queue)

        callback = self._middleware.__class__.generate_callback(
            self.__handle_message,
            self._reciving_queue,
        )
        self._middleware.attach_callback(
            self._reciving_queue, callback
        )

        self._middleware.turn_fair_dispatch()
        try: 
            self._middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?  
            if not self._got_sigterm:
                logging.error(e)

    def __handle_end_transmission(self, body: List[str]):
        # Si me llego un END...
        # 1) Me fijo si los la cantidad de ids que hay es igual a
        # la cantidad total de instancias de mi mismo que hay.
        # Si es asi => Envio el END a la proxima cola
        # Si no es asi => Checkeo si mi ID esta en la lista
        #     Si es asi => No agrego nada y reencolo
        #     Si no es asi => Agrego mi id a la lista y reencolo
        peers_that_recived_end = body[1:]
        if len(peers_that_recived_end) == self._instances_of_myself:
            logging.debug("Sending REAL END")
            self._middleware.send_end(queue=self._forwarding_queue_name)
        else:
            self._middleware.publish_batch(self._forwarding_queue)
            message = [END_TRANSMISSION_MESSAGE]
            if not self._node_id in peers_that_recived_end:
                peers_that_recived_end.append(self._node_id)

            message += peers_that_recived_end 
            self._middleware.publish_message(message, self._reciving_queue_name)


    def __handle_message(
        self,
        delivery_tag: int,
        body: bytes,
    ):
        body = self._middleware.get_rows_from_message(body)
        for message in body:
            logging.debug(f"Recived message: {message} from {self._reciving_queue}")

            if message[0] == END_TRANSMISSION_MESSAGE:
                logging.debug(f"Recived END from: {self._reciving_queue}")

                self.__handle_end_transmission(message)
                self._middleware.ack(delivery_tag)

                return

            columns_to_keep = self._columns_to_keep
            filtered_body = self.__filter_columns(columns_to_keep, message)

            logging.debug(
                f"Sending: {message} to {self._forwarding_queue}"
            )

            self._middleware.publish(
                filtered_body, 
                self._forwarding_queue_name, 
                ""
            )

        self._middleware.ack(delivery_tag)

    def __filter_columns(self, columns_to_keep: List[int], data: List[str]):
        return [data[i].strip() for i in columns_to_keep]

    def __signal_handler(self, sig, frame):
        logging.debug(f"Gracefully shutting down...")
        self._got_sigterm = True 
        self._middleware.shutdown()
