import signal
import logging
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import *
from common.protocol.protocol import Protocol

END_TRANSMISSION_MESSAGE = "END"


class CounterByPlatform:

    def __init__(self, config, middleware: Middleware, protocol: Protocol):
        self._protocol = protocol
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

        self._middleware.attach_callback(consume_queue_name, self.handle_message)

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
                # TODO: If got_sigterm is showing any error needed?  
            if not self._got_sigterm:
                logging.error(e)

    def handle_message(self, ch, method, properties, body):
        body = self._middleware.get_rows_from_message(body)
        for message in body:
            message = [value.strip() for value in message]

            logging.debug(f"GOT MSG: {message}")

            if len(message) == 1 and message[0] == "END":
                self.send_results()
                self._middleware.ack(method.delivery_tag)
                return

            field_to_count = message[0]
            self.count(field_to_count)

        self._middleware.ack(method.delivery_tag)

    def send_results(self):
        # TODO: READ FROM STORAGE

        for key, value in self._count_dict.items():
            if self._got_sigterm:
                return

            # encoded_msg = self.protocol.encode([key, str(value)])
            self._middleware.publish(
                [key, str(value)], queue_name=self._config["PUBLISH_QUEUE"]
            )

        # encoded_msg = self.protocol.encode([END_TRANSMISSION_MESSAGE])
        # self.middleware.publish(encoded_msg, queue_name=self.config["PUBLISH_QUEUE"])
        self._middleware.publish_batch(self._config["PUBLISH_QUEUE"])
        self._middleware.send_end(
            queue=self._config["PUBLISH_QUEUE"],
        )
        logging.debug("Sent results")

    def count(self, field_to_count):
        # TODO: SAVE AND UPDATE ON STORAGE

        actual_count = self._count_dict.get(field_to_count, 0)
        self._count_dict[field_to_count] = actual_count + 1

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        self._middleware.shutdown()
