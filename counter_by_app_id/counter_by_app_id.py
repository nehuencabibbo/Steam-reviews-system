import signal
import logging
from common.middleware.middleware import Middleware
from common.storage.storage import *
from common.protocol.protocol import Protocol
from common.storage import storage

END_TRANSMISSION_MESSAGE = "END"


class CounterByAppId:

    def __init__(self, config, middleware: Middleware, protocol: Protocol):
        self.protocol = protocol
        self.config = config
        self.middleware = middleware
        self.got_sigterm = False
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):

        consume_queue_name = (
            f"{self.config['NODE_ID']}_{self.config['CONSUME_QUEUE_SUFIX']}"
        )
        self.middleware.create_queue(consume_queue_name)
        self.middleware.create_queue(self.config["PUBLISH_QUEUE"])

        self.middleware.attach_callback(consume_queue_name, self.handle_message)

        try:
            logging.debug("Starting to consume...")
            self.middleware.start_consuming()
        except OSError as _:
            if not self.got_sigterm:
                raise

    def handle_message(self, ch, method, properties, body):

        body = self.protocol.decode(body)
        body = [value.strip() for value in body]

        logging.debug(f"GOT MSG: {body}")

        if len(body) == 1 and body[0] == "END":
            self.send_results()
            self.middleware.ack(method.delivery_tag)
            return

        record = f"{body[0]},{1}"
        storage.sum_to_record(
            self.config["STORAGE_DIR"], self.config["RANGE_FOR_PARTITION"], record
        )

        self.middleware.ack(method.delivery_tag)

    def send_results(self):

        reader = storage.read_all_files(self.config["STORAGE_DIR"])
        for record in reader:
            message = record[0].split(",")
            logging.debug(f"Sending: {message}")
            encoded_msg = self.protocol.encode(message)
            self.middleware.publish(
                encoded_msg, queue_name=self.config["PUBLISH_QUEUE"]
            )

        logging.debug("SENDING END")
        encoded_msg = self.protocol.encode([END_TRANSMISSION_MESSAGE])
        self.middleware.publish(encoded_msg, queue_name=self.config["PUBLISH_QUEUE"])
        logging.debug(f'END SENT TO: {self.config["PUBLISH_QUEUE"]}')

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self.got_sigterm = True
        self.middleware.shutdown()
