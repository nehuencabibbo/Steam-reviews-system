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

class FilterColumnByValue(): 
    def __init__(self, protocol: Protocol, middleware: Middleware, config: Dict[str, Union[str, int]]):
        self._protocol = protocol
        self._middleware = middleware
        self._config = config

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def start(self):
        # Reciving queues
        self._middleware.create_queue(self._config['RECIVING_QUEUE_NAME'])
        
        # Forwarding queues
        if self._config["AMOUNT_OF_FORWARDING_QUEUES"] == 1:
            self._middleware.create_queue(self._config['FORWARDING_QUEUE_NAME'])  
        else: 
            for i in range(1, self._config["AMOUNT_OF_FORWARDING_QUEUES"] + 1):
                self._middleware.create_queue(f"{i}_{self._config['FORWARDING_QUEUE_NAME']}")  

        # Attaching callback functions 
        callback = self._middleware.__class__.generate_callback(
            self.__handle_message,
        )
        self._middleware.attach_callback(self._config['RECIVING_QUEUE_NAME'], callback)

        self._middleware.start_consuming()

    def __handle_end_transmission(self):
        encoded_message = self._protocol.encode([END_TRANSMISSION_MESSAGE])
        if self._conifg["AMOUNT_OF_FORWARDING_QUEUES"] == 1:
            self._middleware.publish(
                encoded_message, 
                self._config["FORWARDING_QUEUE_NAME"]
            )
        else: 
            for i in range(1, self._config["AMOUNT_OF_FORWARDING_QUEUES"] + 1):
                self._middleware.publish(
                    encoded_message, 
                    f"{i}_{self._config['FORWARDING_QUEUE_NAME']}"
                )

    def __handle_message(self, delivery_tag: int, body: bytes):
        body = self._protocol.decode(body)
        body = [value.strip() for value in body]
        # Handle END message 
        if len(body) == 1 and body[0] == END_TRANSMISSION_MESSAGE:
            self.__handle_end_transmission()

            self._middleware.ack(delivery_tag)

            return
        
        self.__filter_according_to_criteria()

        self._middleware.ack(delivery_tag)

    def __filter_according_to_criteria(self, body: List[str]):
        column_to_use = body[self._config["COLUMN_NUMBER_TO_USE"]]
        criteria = self._config["CRITERIA"]

        if criteria == EQUAL_CRITERIA_KEYWORD:
            if column_to_use == self._config["VALUE_TO_FILTER_BY"]:
                self.__send_message(body)

        elif criteria == GRATER_THAN_CRITERIA_KEYWORD:
            if int(column_to_use) > self._config["VALUE_TO_FILTER_BY"]:
                self.__send_message(body)

        elif criteria == CONTAINS_CRITERIA_KEYWORD: 
            if column_to_use.contains(self._config["VALUE_TO_FILTER_BY"]):
                self.__send_message(body)

        elif criteria == LANGUAGE_CRITERIA_KEYWORD:
            detected_language, _ = langid.classify(column_to_use)
            if detected_language == self._config["VALUE_TO_FILTER_BY"].lower():
                self.__send_message(body)
        else:
            raise Exception(f'Unkown cirteria: {criteria}')

    def __send_message(self, message: List[str]):
        '''
        Do not use to send END message as it is handled differently.
        '''
        encoded_message = self._protocol.encode(message)
        if self._config["AMOUNT_OF_FORWARDING_QUEUES"] == 1:
            self._middleware.publish(
                encoded_message, 
                self._config["FORWARDING_QUEUE_NAME"]
            )
        else: 
            # TODO: Change with app id when available 
            node_id = node_id_to_send_to(
                '1', 
                message[APP_ID], 
                self._config["AMOUNT_OF_FORWARDING_QUEUES"]
            )
        
            self._middleware.publish(
                encoded_message, 
                f"{node_id}_{self._config['FORWARDING_QUEUE_NAME']}"
            )

    def __signal_handler(self, sig, frame):
        logging.debug("Gracefully shutting down...")
        self._middleware.shutdown()