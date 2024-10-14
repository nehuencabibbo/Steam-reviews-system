import signal
import logging
from typing import * 

from common.middleware.middleware import Middleware, MiddlewareError
from common.protocol.protocol import Protocol
from common.storage import storage
from utils.utils import node_id_to_send_to

END_TRANSMISSION_MESSAGE = "END"
APP_ID = 0


class CounterByAppId:

    def __init__(self, config, middleware: Middleware, protocol: Protocol):
        self._config = config
        self._middleware = middleware
        self._got_sigterm = False
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):

        consume_queue_name = (
            f"{self._config['NODE_ID']}_{self._config['CONSUME_QUEUE_SUFIX']}"
        )
        self._middleware.create_queue(consume_queue_name)
        for i in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
            self._middleware.create_queue(f'{i}_{self._config["PUBLISH_QUEUE"]}')

        self._middleware.attach_callback(consume_queue_name, self.handle_message)

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)

        logging.info("Finished")

    def handle_message(self, ch, method, properties, body):

        body = self._middleware.get_rows_from_message(body)

        logging.debug(f"GOT MSG: {body}")

        if len(body) == 1 and body[0][0] == END_TRANSMISSION_MESSAGE:
            self.send_results()
            self._middleware.ack(method.delivery_tag)
            return

        count_per_record = {}
        for record in body:
            record_id = record[0]
            count_per_record[record_id] = count_per_record.get(record_id, 0) + 1

        storage.sum_batch_to_records(
            self._config["STORAGE_DIR"],
            self._config["RANGE_FOR_PARTITION"],
            count_per_record,
        )

        self._middleware.ack(method.delivery_tag)

    def send_results(self):

        queue_name = self._config["PUBLISH_QUEUE"]

        reader = storage.read_all_files(self._config["STORAGE_DIR"])

        for record in reader:
            self.__send_record_to_forwarding_queues()

            # self._middleware.publish(record, queue_name)

        self.__send_last_batch_to_forwarding_queues()
        self.__send_end_to_forwarding_queues(prefix_queue_name=queue_name)

        # if not delete_directory(self._config["STORAGE_DIR"]):
        #     logging.debug(f"Couldn't delete directory: {self._config["STORAGE_DIR"]}")
        # else:
        #     logging.debug(f"Deleted directory: {self._config["STORAGE_DIR"]}")
        # encoded_msg = self._protocol.encode([END_TRANSMISSION_MESSAGE])
        # self._middleware.publish(encoded_msg, queue_name=self._config["PUBLISH_QUEUE"])
        # logging.debug(f'END SENT TO: {self._config["PUBLISH_QUEUE"]}')

    def __send_record_to_forwarding_queues(self, record: List[str]):
        for queue_number in self._config["AMOUNT_OF_FORWARDING_QUEUES"]:
            full_queue_name = f'{queue_number}_{self._config["PUBLISH_QUEUE"]}'

            logging.debug(f"Sending record: {record} to queue: {full_queue_name}")

            self._middleware.publish(record, full_queue_name)

    def __send_last_batch_to_forwarding_queues(self):
        for queue_number in self._config["AMOUNT_OF_FORWARDING_QUEUES"]:   
            full_queue_name = f'{queue_number}_{self._config["PUBLISH_QUEUE"]}'

            logging.debug(f'Sending last batch to queue: {full_queue_name}')

            self._middleware.publish_batch(full_queue_name)

    def __send_end_to_forwarding_queues(self, prefix_queue_name):
        for i in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
            self._middleware.send_end(f'{i}_{prefix_queue_name}')

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")

        self._got_sigterm = True
        self._middleware.shutdown()
