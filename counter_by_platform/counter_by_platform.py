import signal
import logging
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import *
from common.protocol.protocol import Protocol
from typing import * 

END_TRANSMISSION_MESSAGE = "END"

BOCA = 1
END_TRANSMISSION_SESSION_ID = 0

REGULAR_MESSAGE_SESSION_ID = 0
REGULAR_MESSAGE_FIELD_TO_COUNT_BY = 1 

class CounterByPlatform:

    def __init__(self, config, middleware: Middleware):
        self._config = config
        self._middleware = middleware
        self._got_sigterm = False
        self._count_dict = {}
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):

        consume_queue_name = (
            f"{self._config['NODE_ID']}_{self._config['CONSUME_QUEUE_SUFIX']}"
        )
        self._middleware.create_queue(consume_queue_name)
        self._middleware.create_queue(self._config["PUBLISH_QUEUE"])

        callback = self._middleware.generate_callback(
            self.__handle_message
        )

        self._middleware.attach_callback(consume_queue_name, callback)

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)

    def __handle_message(self, delivery_tag: int, body: List[List[str]]):
        body = self._middleware.get_rows_from_message(body)
        for message in body:

            logging.debug(f"GOT MSG: {message}")

            if message[BOCA] == END_TRANSMISSION_MESSAGE:
                logging.debug('Recived END transmssion')
                session_id = message[END_TRANSMISSION_SESSION_ID]

                self.__send_results(session_id)
                self._middleware.ack(delivery_tag)

                return
            
            logging.debug(message)
            session_id = message[REGULAR_MESSAGE_SESSION_ID]
            field_to_count = message[REGULAR_MESSAGE_FIELD_TO_COUNT_BY]

            self.__count(field_to_count, session_id)

        self._middleware.ack(delivery_tag)


    def __send_results(self, session_id: str):
        # TODO: READ FROM STORAGE
        logging.debug(self._count_dict)
        publish_queue = f'{self._config["PUBLISH_QUEUE"]}_{session_id}'

        logging.debug(self._count_dict[session_id].items())
        self._middleware.create_queue(publish_queue)
        for platform, count in self._count_dict[session_id].items():
            logging.debug(f"{platform}, {count}")
            if self._got_sigterm:
                return

            # encoded_msg = self.protocol.encode([key, str(value)])
            self._middleware.publish(
                [platform, str(count)], queue_name=publish_queue
            )

        # encoded_msg = self.protocol.encode([END_TRANSMISSION_MESSAGE])
        # self.middleware.publish(encoded_msg, queue_name=self.config["PUBLISH_QUEUE"])
        self._middleware.publish_batch(publish_queue)
        self._middleware.send_end(
            queue=publish_queue,
        )
        logging.debug(f"Sent results to queue {publish_queue}")

    def __count(self, field_to_count: str, session_id: str):
        # TODO: SAVE AND UPDATE ON STORAGE

        # session_id = {windows: algo, mac: algo, linux: algo}

        if not session_id in self._count_dict: 
            self._count_dict[session_id] = {}

        session_id_field_count = self._count_dict[session_id].get(field_to_count, 0)

        self._count_dict[session_id][field_to_count] = session_id_field_count + 1

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        self._middleware.shutdown()
