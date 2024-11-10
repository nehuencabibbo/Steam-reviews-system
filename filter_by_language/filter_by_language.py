import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import *
from constants import *
from common.protocol.protocol import Protocol
from common.middleware.middleware import Middleware, MiddlewareError
from utils.utils import node_id_to_send_to

import signal
import logging
import langid


class FilterByLanguage:
    def __init__(
        self,
        protocol: Protocol,
        middleware: Middleware,
        config: Dict[str, Union[str, int]],
    ):
        self._protocol = protocol
        self._middleware = middleware
        self._got_sigterm = False
        self._filter_by_criteria: Callable[[List[str]], None] = None

        # Config variables
        # Config variables that are recurrently accesed in callback functions are
        # stored so there's no unnecesary key hashing for dictionary access each
        # time the callback function is called
        self._forwarding_queue_names: List[str] = config["FORWARDING_QUEUE_NAMES"]
        self._amount_of_forwarding_queues: List[int] = config[
            "AMOUNT_OF_FORWARDING_QUEUES"
        ]
        self._columns_to_keep: List[int] = config["COLUMNS_TO_KEEP"]
        self._column_number_to_use: int = config["COLUMN_NUMBER_TO_USE"]
        self._node_id: str = config["NODE_ID"]
        self._receiving_queue_name = config["RECIVING_QUEUE_NAME"]
        self._instances_of_myself = config["INSTANCES_OF_MYSELF"]

        self._language = config["LANGUAGE"]

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def start(self):
        # Reciving queues
        self._middleware.create_queue(self._receiving_queue_name)

        # Forwarding queues
        self.__create_all_forwarding_queues()

        # Attaching callback functions
        callback = self._middleware.__class__.generate_callback(
            self.__handle_message,
        )
        self._middleware.attach_callback(self._receiving_queue_name, callback)

        try:
            self._middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self._middleware.shutdown()

    def __create_all_forwarding_queues(self):
        """
        If amount_of_forwarding_queues = [2, 3] and forwarding_queue_names = ['pablo', 'rabbit']
        then the following queues will be created:

        - 0_pablo
        - 1_pablo
        - 0_rabbit
        - 1_rabbit
        - 2_rabbit
        """
        for i in range(len(self._amount_of_forwarding_queues)):
            queues_to_create = self._amount_of_forwarding_queues[i]
            queue_name = self._forwarding_queue_names[i]

            for queue_number in range(queues_to_create):
                full_queue_name = f"{queue_number}_{queue_name}"
                self._middleware.create_queue(full_queue_name)

    def __handle_end_transmission(self, body: List[str]):
        # Si me llego un END...
        # 1) Me fijo si los la cantidad de ids que hay es igual a
        # la cantidad total de instancias de mi mismo que hay.
        # Si es asi => Envio el END a la proxima cola
        # Si no es asi => Checkeo si mi ID esta en la lista
        #     Si es asi => No agrego nada y reencolo
        #     Si no es asi => Agrego mi id a la lista y reencolo

        peers_that_recived_end = body[2:]
        client_id = body[0]

        if len(peers_that_recived_end) == int(self._instances_of_myself):
            self.__send_end_transmission_to_all_forwarding_queues(client_id)
        else:

            message = [client_id, END_TRANSMISSION_MESSAGE]
            if not self._node_id in peers_that_recived_end:
                peers_that_recived_end.append(self._node_id)

            message += peers_that_recived_end

            self._middleware.publish_message(message, self._receiving_queue_name)

    def __send_last_batch_to_fowarding_queues(self):
        # TODO: Repeated code between this and send end, remove
        for i in range(len(self._amount_of_forwarding_queues)):
            amount_of_current_queue = self._amount_of_forwarding_queues[i]
            queue_name = self._forwarding_queue_names[i]

            logging.debug(f"AMOUNT_OF_CURRENT_QUEUE: {amount_of_current_queue}")
            for queue_number in range(amount_of_current_queue):
                full_queue_name = f"{queue_number}_{queue_name}"
                logging.debug(f"Sending last batch to queue: {full_queue_name}")

                self._middleware.publish_batch(full_queue_name)

    def __send_end_transmission_to_all_forwarding_queues(self, client_id: str):
        # TODO: Repeated code between this and send last batch, remove
        for i in range(len(self._amount_of_forwarding_queues)):
            amount_of_current_queue = self._amount_of_forwarding_queues[i]
            queue_name = self._forwarding_queue_names[i]

            for queue_number in range(amount_of_current_queue):
                full_queue_name = f"{queue_number}_{queue_name}"
                logging.debug(f"Sending END to queue: {full_queue_name}")

                self._middleware.send_end(
                    queue=full_queue_name,
                    end_message=[client_id, END_TRANSMISSION_MESSAGE],
                )

    def __handle_message(self, delivery_tag: int, body: bytes):
        body = self._middleware.get_rows_from_message(body)
        for message in body:
            logging.debug(f"Recived message: {message}")

            if message[1] == END_TRANSMISSION_MESSAGE:
                logging.debug(f"GOT END: {body}")
                self.__send_last_batch_to_fowarding_queues()
                self.__handle_end_transmission(message)
                self._middleware.ack(delivery_tag)

                return

            self.__filter_language(message)

        self._middleware.ack(delivery_tag)

    def __filter_language(self, body: List[str]):
        column_to_use = body[self._column_number_to_use]
        detected_language, _ = langid.classify(column_to_use)
        if detected_language == self._language.lower():
            self.__send_message(body)

    def __filter_columns(self, data: List[str]):
        # No filter needed
        if len(self._columns_to_keep) == 1 and self._columns_to_keep[0] == -1:
            return data

        return [data[i] for i in self._columns_to_keep]

    def __send_message(self, message: List[str]):
        """
        Do not use to send END message as it is handled differently.
        """
        message = self.__filter_columns(message)
        client_id = message[CLIENT_ID]

        for i in range(len(self._amount_of_forwarding_queues)):
            amount_of_current_queue = self._amount_of_forwarding_queues[i]
            queue_name = self._forwarding_queue_names[i]

            node_id = node_id_to_send_to(
                client_id, message[APP_ID], amount_of_current_queue
            )

            queue_to_send_to = f"{node_id}_{queue_name}"

            logging.debug(f"Sending message: {message} to queue: {queue_to_send_to}")
            # TODO: Use batches here?
            self._middleware.publish(message, queue_to_send_to)

    def __signal_handler(self, sig, frame):
        logging.debug("Gracefully shutting down...")
        self._got_sigterm = True
        self._middleware.stop_consuming_gracefully()
        # self._middleware.shutdown()
