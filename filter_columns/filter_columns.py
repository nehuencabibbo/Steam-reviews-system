import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import *
from middleware.middleware import Middleware
import signal 
import logging

GAMES_MESSAGE_TYPE = 'games'
REVIEWS_MESSAGE_TYPE = 'reviews'
END_TRANMISSION_MESSAGE = 'END' 

class FilterColumns(): 
    def __init__(self, middleware: Middleware, config: Dict[str, Union[str, int]]):
        self._middleware = middleware
        self._config = config

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def start(self):
        # Queues that the client uses to send data
        self._middleware.create_queue(self._config['CLIENT_GAMES_QUEUE_NAME'])
        self._middleware.create_queue(self._config['CLIENT_REVIEWS_QUEUE_NAME'])
        # Queues that filter columns uses to send data to null drop
        self._middleware.create_queue(self._config['NULL_DROP_GAMES_QUEUE_NAME'])
        self._middleware.create_queue(self._config['NULL_DROP_REVIEWS_QUEUE_NAME'])

        games_callback = self._middleware.__class__.generate_callback(
            self.__handle_message, 
            GAMES_MESSAGE_TYPE,
            self._config['NULL_DROP_GAMES_QUEUE_NAME']
        )
        self._middleware.attach_callback(self._config['CLIENT_GAMES_QUEUE_NAME'], games_callback)

        reviews_callback = self._middleware.__class__.generate_callback(
            self.__handle_message, 
            REVIEWS_MESSAGE_TYPE,
            self._config['NULL_DROP_REVIEWS_QUEUE_NAME']
        )
        self._middleware.attach_callback(self._config['CLIENT_REVIEWS_QUEUE_NAME'], reviews_callback)

        self._middleware.start_consuming()

    def __handle_message(self, delivery_tag: int, body: bytes, message_type: str, forwarding_queue_name: str):
        body = body.decode('utf-8')
        if body == END_TRANMISSION_MESSAGE:
            self._middleware.publish(body, forwarding_queue_name, '')
            self._middleware.ack(delivery_tag)

            return
        
        body = body.split(',')
        body = [value.strip() for value in body]
        logging.debug(f"[FILTER COLUMNS {self._config['NODE_ID']}] Recived {message_type}: {body}")

        columns_to_keep = []
        if message_type == GAMES_MESSAGE_TYPE:
            columns_to_keep = self._config['GAMES_COLUMNS_TO_KEEP']
        elif message_type == REVIEWS_MESSAGE_TYPE:
            columns_to_keep = self._config['REVIEWS_COLUMNS_TO_KEEP']
        else: 
            # Message type was not set properly, unrecoverable error 
            raise Exception(f'[ERROR] Unkown message type {message_type}') 

        filtered_body = self.__filter_columns(columns_to_keep, body)
        filtered_body = ','.join(filtered_body)
        logging.debug(f"[FILTER COLUMNS {self._config['NODE_ID']}] Sending {message_type}: {body}")
    
        self._middleware.publish(filtered_body, forwarding_queue_name, '')

        self._middleware.ack(delivery_tag)

    def __filter_columns(self, columns_to_keep: List[int], data: List[str]):
        return [data[i] for i in columns_to_keep]

    def __signal_handler(self, sig, frame):
        logging.debug(f"[FILTER COLUMNS {self._config['NODE_ID']}] Gracefully shutting down...")
        self.middleware.shutdown()