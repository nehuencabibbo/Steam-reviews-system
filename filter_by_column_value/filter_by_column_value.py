import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import *
from constants import *
from common.protocol.protocol import Protocol
from middleware.middleware import Middleware
from utils.utils import node_id_to_send_to

import signal
import logging
import langid


class FilterColumnByValue:
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
        self._middleware.create_queue(self._config["RECIVING_QUEUE_NAME"])

        # Forwarding queues
        for i in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
            self._middleware.create_queue(
                f"{i}_{self._config['FORWARDING_QUEUE_NAME']}"
            )

        # Attaching callback functions
        callback = self._middleware.__class__.generate_callback(
            self.__handle_message,
        )
        self._middleware.attach_callback(self._config["RECIVING_QUEUE_NAME"], callback)

        self._middleware.start_consuming()

    def __handle_end_transmission(self, body: List[str]):
        # Si me llego un END...
        # 1) Me fijo si los la cantidad de ids que hay es igual a 
        # la cantidad total de instancias de mi mismo que hay.
        # Si es asi => Envio el END a la proxima cola
        # Si no es asi => Checkeo si mi ID esta en la lista
        #     Si es asi => No agrego nada y reencolo
        #     Si no es asi => Agrego mi id a la lista y reencolo
        peers_that_recived_end = body[1:]
        if len(peers_that_recived_end) == int(self._config["INSTANCES_OF_MYSELF"]):
            encoded_message = self._protocol.encode_batch([[END_TRANSMISSION_MESSAGE]])
            self.__send_end_transmission_to_all_forwarding_queues()

        else: 
            message = [END_TRANSMISSION_MESSAGE]
            if not self._config["NODE_ID"] in peers_that_recived_end:
                peers_that_recived_end.append(self._config["NODE_ID"])
            
            message += peers_that_recived_end
            encoded_message = self._protocol.encode_batch([message])
            self._middleware.publish(
                encoded_message, 
                self._config["RECIVING_QUEUE_NAME"], 
                ""
            )

    def __send_end_transmission_to_all_forwarding_queues(self):
        encoded_message = self._protocol.encode_batch([[END_TRANSMISSION_MESSAGE]])

        # TODO: Verify that EVERY queue is started at 0
        for i in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
            self._middleware.publish(
                encoded_message, f"{i}_{self._config['FORWARDING_QUEUE_NAME']}"
            )

    def __handle_message(self, delivery_tag: int, body: bytes):
        body = self._protocol.decode_batch(body)
        body = [[value.strip() for value in message] for message in body]

        logging.debug(f"Recived message: {body}")
        if body[0][0] == END_TRANSMISSION_MESSAGE:
            self.__handle_end_transmission(body[0])

            self._middleware.ack(delivery_tag)

            return

        self.__filter_according_to_criteria(body)

        self._middleware.ack(delivery_tag)

    def __filter_according_to_criteria(self, body: List[List[str]]):
        
        batch = {}
        for message in body:
            # TODO: Change with client id when available
            node_id = node_id_to_send_to(
                "1", message[APP_ID], self._config["AMOUNT_OF_FORWARDING_QUEUES"]
            )
            batch[node_id] = batch.get(node_id, []) #create list if node was not a key

            column_to_use = message[self._config["COLUMN_NUMBER_TO_USE"]]
            value_to_filter_by = self._config["VALUE_TO_FILTER_BY"]
            criteria = self._config["CRITERIA"]

            if criteria == EQUAL_CRITERIA_KEYWORD:
                if column_to_use == value_to_filter_by:
                    
                    batch[node_id].append(message)

            elif criteria == GRATER_THAN_CRITERIA_KEYWORD:
                if int(column_to_use) > int(value_to_filter_by):
                    batch[node_id].append(message)

            elif criteria == CONTAINS_CRITERIA_KEYWORD:
                if value_to_filter_by in column_to_use.lower():
                    batch[node_id].append(message)

            elif criteria == LANGUAGE_CRITERIA_KEYWORD:
                detected_language, _ = langid.classify(column_to_use)
                if detected_language == value_to_filter_by.lower():
                    batch[node_id].append(message)
            else:
                raise Exception(f"Unkown cirteria: {criteria}")
            
        if len(batch) > 0: self.__send_message(batch)

    def __filter_columns(self, columns_to_keep: List[int], data: List[List[str]]):
        # No filter needed
        if len(columns_to_keep) == 1 and columns_to_keep[0] == -1:
            return data

        filtered_batch = []
        for message in data:
            filtered_batch.append([message[i] for i in columns_to_keep])

        return filtered_batch

    def __send_message(self, message: Dict[int, List[str]]):
        """
        Do not use to send END message as it is handled differently.
        """
        for node_id, batch in message.items():

            batch = self.__filter_columns(self._config["COLUMNS_TO_KEEP"], batch)
            if len(batch) > 0:
               
                logging.debug(f"Sending message: {batch}")
                encoded_message = self._protocol.encode_batch(batch)

                self._middleware.publish(
                    encoded_message, f'{node_id}_{self._config["FORWARDING_QUEUE_NAME"]}'
                )



    def __signal_handler(self, sig, frame):
        logging.debug("Gracefully shutting down...")
        self._middleware.shutdown()
