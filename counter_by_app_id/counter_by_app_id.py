import signal
import logging
from typing import *

from common.middleware.middleware import Middleware, MiddlewareError
from common.protocol.protocol import Protocol
from common.storage import storage
from utils.utils import node_id_to_send_to

END_TRANSMISSION_MESSAGE = "END"

END_MESSAGE_CLIENT_ID = 0
END_MESSAGE_END = 1

REGULAR_MESSAGE_CLIENT_ID = 0
REGULAR_MESSAGE_APP_ID = 1


class CounterByAppId:

    def __init__(self, config, middleware: Middleware, protocol: Protocol):
        self._config = config
        self._middleware = middleware
        self._got_sigterm = False
        self._ends_received_per_client = {}
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):
        # Creating reciving queue
        consume_queue_name = (
            f"{self._config['NODE_ID']}_{self._config['CONSUME_QUEUE_SUFIX']}"
        )
        self._middleware.create_queue(consume_queue_name)

        # Creating forwarding queues
        self.__create_all_forwarding_queues()

        callback = self._middleware.__class__.generate_callback(self.__handle_message)
        self._middleware.attach_callback(consume_queue_name, callback)

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)

        logging.info("Finished")

    def __create_all_forwarding_queues(self):
        for i in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
            self._middleware.create_queue(f'{i}_{self._config["PUBLISH_QUEUE"]}')

    def __handle_message(self, delivery_tag: int, body: List[List[str]]):

        body = self._middleware.get_rows_from_message(body)

        logging.debug(f"GOT MSG: {body}")

        if body[0][END_MESSAGE_END] == END_TRANSMISSION_MESSAGE:
            client_id = body[0][END_MESSAGE_CLIENT_ID]
            self._ends_received_per_client[client_id] = (
                self._ends_received_per_client.get(client_id, 0) + 1
            )

            logging.debug(
                f"Amount of ends received up to now: {self._ends_received_per_client[client_id]} | Expecting: {self._config['NEEDED_ENDS']}"
            )
            if self._ends_received_per_client[client_id] == self._config["NEEDED_ENDS"]:

                self.__send_results(client_id)

            self._middleware.ack(delivery_tag)
            return

        # client_id: {}
        count_per_record_by_client_id = {}
        for record in body:
            client_id = record[REGULAR_MESSAGE_CLIENT_ID]
            record_id = record[REGULAR_MESSAGE_APP_ID]

            if not client_id in count_per_record_by_client_id:
                count_per_record_by_client_id[client_id] = {}

            count_per_record_by_client_id[client_id][record_id] = (
                count_per_record_by_client_id[client_id].get(record_id, 0) + 1
            )

        storage.sum_batch_to_records_per_client(
            self._config["STORAGE_DIR"],
            self._config["RANGE_FOR_PARTITION"],
            count_per_record_by_client_id,
        )

        self._middleware.ack(delivery_tag)

    def __send_results(self, client_id: str):
        queue_name = self._config["PUBLISH_QUEUE"]

        storage_dir = f'{self._config["STORAGE_DIR"]}/{client_id}'
        reader = storage.read_all_files(storage_dir)

        for record in reader:
            logging.debug(f"RECORD READ: {record}")
            record.insert(0, client_id)
            self.__send_record_to_forwarding_queues(record)

        self.__send_last_batch_to_forwarding_queues()
        self.__send_end_to_forwarding_queues(
            prefix_queue_name=queue_name, client_id=client_id
        )

        self._clear_client_data(client_id, storage_dir)


    def __send_record_to_forwarding_queues(self, record: List[str]):
        for queue_number in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
            full_queue_name = f'{queue_number}_{self._config["PUBLISH_QUEUE"]}'

            logging.debug(f"Sending record: {record} to queue: {full_queue_name}")

            self._middleware.publish(record, full_queue_name)

    def __send_last_batch_to_forwarding_queues(self):
        for queue_number in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
            full_queue_name = f'{queue_number}_{self._config["PUBLISH_QUEUE"]}'

            logging.debug(f"Sending last batch to queue: {full_queue_name}")

            self._middleware.publish_batch(full_queue_name)

    def __send_end_to_forwarding_queues(self, prefix_queue_name, client_id):
        for i in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
            self._middleware.send_end(
                f"{i}_{prefix_queue_name}",
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )
            logging.debug(f"Sent END of client: {client_id}")


    def _clear_client_data(self, client_id, storage_dir):
        if not storage.delete_directory(storage_dir):
            logging.debug(f"Couldn't delete directory: {storage_dir}")
        else:
            logging.debug(f"Deleted directory: {storage_dir}")
        self._ends_received_per_client.pop(client_id)


    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")

        self._got_sigterm = True
        self._middleware.shutdown()
