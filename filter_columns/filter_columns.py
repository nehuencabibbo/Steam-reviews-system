import signal 
import logging


class FilterColumns(): 
    def __init__(self, middleware, config):
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
            self._config['CLIENT_GAMES_QUEUE_NAME'], 
            self._config['NULL_DROP_GAMES_QUEUE_NAME']
        )
        self._middleware.attach_callback('games', games_callback)

        reviews_callback = self._middleware.__class__.generate_callback(
            self.__handle_message, 
            self._config['CLIENT_REVIEWS_QUEUE_NAME'], 
            self._config['NULL_DROP_REVIEWS_QUEUE_NAME']
        )
        self._middleware.attach_callback('reviews', reviews_callback)

        self._middleware.start_consuming()

    def __handle_message(self, delivery_tag, body, message_type, forwarding_queue_name):
        body = body.decode('utf-8').split(',')
        body = [value.strip() for value in body]
        logging.debug(f"[FILTER COLUMNS {self._config['NODE_ID']}] Recived {message_type}: {body}")

        columns_to_keep = []
        if message_type == 'games':
            columns_to_keep = self._config['GAMES_COLUMNS_TO_KEEP']
        elif message_type == 'reviews':
            columns_to_keep = self._config['REVIEWS_COLUMNS_TO_KEEP']
        else: 
            # Message type was not set properly, unrecoverable error 
            raise Exception(f'[ERROR] Unkown message type {message_type}') 

        filtered_body = self.__filter_columns(columns_to_keep, body)
        filtered_body = ','.join(filtered_body)
        logging.debug(f"[FILTER COLUMNS {self._config['NODE_ID']}] Sending {message_type}: {body}")
    
        self._middleware.publish(filtered_body, forwarding_queue_name, '')

        self._middleware.ack(delivery_tag)

    def __filter_columns(self, columns_to_keep, data):
        return [data[i] for i in columns_to_keep]

    def __signal_handler(self, sig, frame):
        logging.debug(f"[FILTER COLUMNS {self._config['NODE_ID']}] Gracefully shutting down...")
        self.middleware.shutdown()