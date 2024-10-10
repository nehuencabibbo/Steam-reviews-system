import signal
import logging
from common.middleware.middleware import Middleware
from common.storage.storage import *
from common.protocol.protocol import Protocol
from common.storage import storage

END_TRANSMISSION_MESSAGE = "END"


class CounterByAppId:

    def __init__(self, config, middleware: Middleware, protocol: Protocol):
        self._protocol = protocol
        self._config = config
        self._middleware = middleware
        self._got_sigterm = False
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
        except OSError as _:
            if not self._got_sigterm:
                raise

    def handle_message(self, ch, method, properties, body):

        # body = self._protocol.decode(body)
        # body = [value.strip() for value in body]
    
        body = self._middleware.get_rows_from_message(body)

        for message in body:

            logging.debug(f"GOT MSG: {message}")

            if len(message) == 1 and message[0] == "END":
                self.send_results()
                self._middleware.ack(method.delivery_tag)
                return

            record = f"{message[0]},{1}"
            storage.sum_to_record(
                self._config["STORAGE_DIR"], self._config["RANGE_FOR_PARTITION"], record
            )

        self._middleware.ack(method.delivery_tag)

    def send_results(self):

        queue_name = self._config["PUBLISH_QUEUE"]

        reader = storage.read_all_files(self._config["STORAGE_DIR"])
        for record in reader:
            message = record[0].split(",")
            logging.debug(f"Sending: {message}")
            # encoded_msg = self._protocol.encode(message)
            self._middleware.publish(message, queue_name)

        self._middleware.publish_batch(queue_name)

        logging.debug("SENDING END")
        self._middleware.send_end(queue_name)

        # encoded_msg = self._protocol.encode([END_TRANSMISSION_MESSAGE])
        # self._middleware.publish(encoded_msg, queue_name=self._config["PUBLISH_QUEUE"])
        # logging.debug(f'END SENT TO: {self._config["PUBLISH_QUEUE"]}')

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        self._middleware.shutdown()
