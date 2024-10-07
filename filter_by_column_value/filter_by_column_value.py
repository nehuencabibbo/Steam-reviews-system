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
        for i in range(1, self._config["AMOUNT_OF_FORWARDING_QUEUES"] + 1):
            self._middleware.create_queue(
                f"{i}_{self._config['FORWARDING_QUEUE_NAME']}"
            )

        # Attaching callback functions
        callback = self._middleware.__class__.generate_callback(
            self.__handle_message,
        )
        self._middleware.attach_callback(self._config["RECIVING_QUEUE_NAME"], callback)

        self._middleware.start_consuming()

    def __handle_end_transmission(self):
        encoded_message = self._protocol.encode([END_TRANSMISSION_MESSAGE])
        self._middleware.broadcast(
            encoded_message=encoded_message,
            amount_of_nodes=self._config["AMOUNT_OF_FORWARDING_QUEUES"],
            queue_sufix=self._config["FORWARDING_QUEUE_NAME"],
        )
        # for i in range(1, self._config["AMOUNT_OF_FORWARDING_QUEUES"] + 1):
        #     self._middleware.publish(
        #         encoded_message, f"{i}_{self._config['FORWARDING_QUEUE_NAME']}"
        #     )

    def __handle_message(self, delivery_tag: int, body: bytes):
        body = self._protocol.decode(body)
        body = [value.strip() for value in body]
        logging.debug(f"Recived message: {body}")
        # Handle END message
        if len(body) == 1 and body[0] == END_TRANSMISSION_MESSAGE:
            logging.debug("Recived END")
            self.__handle_end_transmission()

            self._middleware.ack(delivery_tag)

            return

        self.__filter_according_to_criteria(body)

        self._middleware.ack(delivery_tag)

    def __filter_according_to_criteria(self, body: List[str]):
        column_to_use = body[self._config["COLUMN_NUMBER_TO_USE"]]
        value_to_filter_by = self._config["VALUE_TO_FILTER_BY"]
        criteria = self._config["CRITERIA"]

        if criteria == EQUAL_CRITERIA_KEYWORD:
            if column_to_use == value_to_filter_by:
                self.__send_message(body)

        elif criteria == GRATER_THAN_CRITERIA_KEYWORD:
            if int(column_to_use) > int(value_to_filter_by):
                self.__send_message(body)

        elif criteria == CONTAINS_CRITERIA_KEYWORD:
            if value_to_filter_by in column_to_use.lower():
                self.__send_message(body)

        elif criteria == LANGUAGE_CRITERIA_KEYWORD:
            detected_language, _ = langid.classify(column_to_use)
            if detected_language == value_to_filter_by.lower():
                self.__send_message(body)
        else:
            raise Exception(f"Unkown cirteria: {criteria}")

    def __filter_columns(self, columns_to_keep: List[int], data: List[str]):
        # No filter needed
        if len(columns_to_keep) == 1 and columns_to_keep[0] == -1:
            return data

        return [data[i] for i in columns_to_keep]

    def __send_message(self, message: List[str]):
        """
        Do not use to send END message as it is handled differently.
        """
        # for i in range(1, self._config["AMOUNT_OF_FORWARDING_QUEUES"] + 1):
        node_id = node_id_to_send_to(
            "1", message[APP_ID], self._config["AMOUNT_OF_FORWARDING_QUEUES"]
        )

        message = self.__filter_columns(self._config["COLUMNS_TO_KEEP"], message)
        logging.debug(
            f"Sending message: {message} to queue: {node_id}_{self._config['FORWARDING_QUEUE_NAME']}"
        )
        encoded_message = self._protocol.encode(message)
        # TODO: Change with app id when available

        self._middleware.publish(
            encoded_message, f"{node_id}_{self._config['FORWARDING_QUEUE_NAME']}"
        )

    def __signal_handler(self, sig, frame):
        logging.debug("Gracefully shutting down...")
        self._middleware.shutdown()
