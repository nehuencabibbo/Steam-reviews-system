import signal
import logging
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from common.protocol.protocol import Protocol
import math

END_TRANSMISSION_MESSAGE = "END"
FILE_NAME = "percentile_data.csv"
NO_RECORDS = 0


class Percentile:

    def __init__(self, config: dict, middleware: Middleware, protocol: Protocol):
        self._config = config
        self._middleware = middleware
        self._got_sigterm = False
        self._amount_msg_received = 0
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):

        self._middleware.create_queue(self._config["CONSUME_QUEUE"])
        self._middleware.create_queue(self._config["PUBLISH_QUEUE"])

        self._middleware.attach_callback(
            self._config["CONSUME_QUEUE"], self.handle_message
        )

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)

    def handle_message(self, ch, method, properties, body):

        body = self._middleware.get_rows_from_message(body)
        logging.debug(f"body: {body}")

        if len(body) == 1 and body[0][0] == END_TRANSMISSION_MESSAGE:
            logging.debug(f"GOT END")
            self.handle_end_message()
            self._middleware.ack(method.delivery_tag)
            return

        logging.debug(f"GOT MSG: {body}")

        self._amount_msg_received += len(body) #this should be saved in case of failure?
        storage.add_batch_to_sorted_file(self._config["STORAGE_DIR"], body)

        self._middleware.ack(method.delivery_tag)


    def handle_end_message(self):

        percentile = self.get_percentile()
        logging.info(f"Percentile is: {percentile}")

        reader = storage.read_sorted_file(self._config["STORAGE_DIR"])
        for row in reader:

            record_value = int(row[1])
            if record_value >= percentile:
                logging.debug(f"Sending: {row}")
                self._middleware.publish(row, self._config["PUBLISH_QUEUE"])

        self._middleware.publish_batch(self._config["PUBLISH_QUEUE"])
        self._middleware.send_end(self._config["PUBLISH_QUEUE"])

        self._amount_msg_received = 0
        # if not storage.delete_directory(self._config["STORAGE_DIR"]):
        #     logging.debug(f"Couldn't delete directory: {self._config["STORAGE_DIR"]}")
        # else:
        #     logging.debug(f"Deleted directory: {self._config["STORAGE_DIR"]}")

    def get_percentile(self):
        rank = (self._config["PERCENTILE"] / 100) * self._amount_msg_received
        rank = math.ceil(rank)  # instead of interpolating, round the number

        logging.debug(f"Ordinal rank is {rank}")

        reader = storage.read_sorted_file(self._config["STORAGE_DIR"])
        for i, row in enumerate(reader):
            if (i + 1) == rank:
                _, value = row
                logging.debug(f"VALUE: {value}")
                return int(value)

        return NO_RECORDS

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        self._middleware.shutdown()
