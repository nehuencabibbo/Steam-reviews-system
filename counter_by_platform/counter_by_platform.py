import signal
import logging
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import *
from common.protocol.protocol import Protocol
from typing import *

END_TRANSMISSION_MESSAGE = "END"

END_TRANSMISSION_MESSAGE_INDEX = 1
END_TRANSMISSION_SESSION_ID = 0

REGULAR_MESSAGE_SESSION_ID = 0
REGULAR_MESSAGE_FIELD_TO_COUNT_BY = 1


class CounterByPlatform:

    def __init__(self, config, middleware: Middleware):
        self._middleware = middleware
        self._got_sigterm = False
        self._count_dict = {}

        # Assigning config values to instance attributes
        self.node_id = config["NODE_ID"]
        self.consume_queue_suffix = config["CONSUME_QUEUE_SUFIX"]
        self.publish_queue = config["PUBLISH_QUEUE"]

        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):
        consume_queue_name = f"{self.node_id}_{self.consume_queue_suffix}"

        self._middleware.create_queue(consume_queue_name)
        self._middleware.create_queue(self.publish_queue)

        callback = self._middleware.generate_callback(self.__handle_message)
        self._middleware.attach_callback(consume_queue_name, callback)

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self._middleware.shutdown()

    def __handle_message(self, delivery_tag: int, body: List[List[str]]):
        body = self._middleware.get_rows_from_message(body)
        for message in body:
            logging.debug(f"GOT MSG: {message}")

            if message[END_TRANSMISSION_MESSAGE_INDEX] == END_TRANSMISSION_MESSAGE:
                logging.debug("Received END transmission")
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
        self._middleware.create_queue(self.publish_queue)

        for platform, count in self._count_dict[session_id].items():
            logging.debug(f"{platform}, {count}")
            if self._got_sigterm:
                return

            self._middleware.publish(
                [session_id, platform, str(count)], queue_name=self.publish_queue
            )

        self._middleware.publish_batch(self.publish_queue)
        self._middleware.send_end(
            queue=self.publish_queue, end_message=[session_id, END_TRANSMISSION_MESSAGE]
        )
        logging.debug(f"Sent results to queue {self.publish_queue}")

    def __count(self, field_to_count: str, session_id: str):
        if not session_id in self._count_dict:
            self._count_dict[session_id] = {}

        session_id_field_count = self._count_dict[session_id].get(field_to_count, 0)
        self._count_dict[session_id][field_to_count] = session_id_field_count + 1

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        self._middleware.stop_consuming_gracefully()
