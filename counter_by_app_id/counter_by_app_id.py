import signal
import logging
from typing import *
import threading

from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from common.watchdog_client.watchdog_client import WatchdogClient

END_TRANSMISSION_MESSAGE = "END"
SESSION_TIMEOUT_MESSAGE = "TIMEOUT"

SESSION_TIMEOUT_MESSAGE_INDEX = 1
END_MESSAGE_CLIENT_ID = 0
END_MESSAGE_END = 1

REGULAR_MESSAGE_CLIENT_ID = 0
REGULAR_MESSAGE_APP_ID = 1


class CounterByAppId:

    def __init__(self, config, middleware: Middleware, monitor: WatchdogClient):
        self._config = config
        self._middleware = middleware
        self._got_sigterm = False
        self._ends_received_per_client = {}
        self._client_monitor = monitor

        # Configuration attributes
        self._node_id = config["NODE_ID"]
        self._consume_queue_suffix = config["CONSUME_QUEUE_SUFIX"]
        self._amount_of_forwarding_queues = config["AMOUNT_OF_FORWARDING_QUEUES"]
        self._publish_queue = config["PUBLISH_QUEUE"]
        self._storage_dir = config["STORAGE_DIR"]
        self._range_for_partition = config["RANGE_FOR_PARTITION"]
        self._needed_ends = config["NEEDED_ENDS"]

        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):

        monitor_thread = threading.Thread(target=self._client_monitor.start)
        monitor_thread.start()

        # Creating receiving queue
        consume_queue_name = f"{self._node_id}_{self._consume_queue_suffix}"
        self._middleware.create_queue(consume_queue_name)

        # Creating forwarding queues
        self.__create_all_forwarding_queues()

        callback = self._middleware.__class__.generate_callback(self.__handle_message)
        self._middleware.attach_callback(consume_queue_name, callback)

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self._middleware.shutdown()
            monitor_thread.join()

        logging.debug("Finished")

    def __create_all_forwarding_queues(self):
        for i in range(self._amount_of_forwarding_queues):
            self._middleware.create_queue(f"{i}_{self._publish_queue}")

    def __handle_message(self, delivery_tag: int, body: List[List[str]]):
        body = self._middleware.get_rows_from_message(body)

        logging.debug(f"GOT MSG: {body}")

        if body[0][SESSION_TIMEOUT_MESSAGE_INDEX] == SESSION_TIMEOUT_MESSAGE:
            session_id = body[0][END_MESSAGE_CLIENT_ID]
            self.__total_timeouts_received_per_client[session_id] = (
                self.__total_timeouts_received_per_client.get(session_id, 0) + 1
            )

            logging.info(
                f"Amount of timeouts received up to now: {self.__total_timeouts_received_per_client[session_id]} | Expecting: {self._needed_ends}"
            )
            if (
                self.__total_timeouts_received_per_client[session_id]
                == self._needed_ends
            ):
                queue_name = self._publish_queue
                storage_dir = f"{self._storage_dir}/{session_id}"

                self.__send_last_batch_to_forwarding_queues()

                self.__send_to_forwarding_queues(
                    prefix_queue_name=queue_name,
                    client_id=session_id,
                    message=SESSION_TIMEOUT_MESSAGE,
                )

                self._clear_client_data(session_id, storage_dir)

            self._middleware.ack(delivery_tag)
            return

        if body[0][END_MESSAGE_END] == END_TRANSMISSION_MESSAGE:
            client_id = body[0][END_MESSAGE_CLIENT_ID]
            self._ends_received_per_client[client_id] = (
                self._ends_received_per_client.get(client_id, 0) + 1
            )

            logging.debug(
                f"Amount of ends received up to now: {self._ends_received_per_client[client_id]} | Expecting: {self._needed_ends}"
            )
            if self._ends_received_per_client[client_id] == self._needed_ends:
                self.__send_results(client_id)

            self._middleware.ack(delivery_tag)
            return

        count_per_record_by_client_id = {}
        for record in body:
            client_id = record[REGULAR_MESSAGE_CLIENT_ID]
            record_id = record[REGULAR_MESSAGE_APP_ID]

            if client_id not in count_per_record_by_client_id:
                count_per_record_by_client_id[client_id] = {}

            count_per_record_by_client_id[client_id][record_id] = (
                count_per_record_by_client_id[client_id].get(record_id, 0) + 1
            )

        storage.sum_batch_to_records_per_client(
            self._storage_dir,
            self._range_for_partition,
            count_per_record_by_client_id,
        )

        self._middleware.ack(delivery_tag)

    def __send_results(self, client_id: str):
        queue_name = self._publish_queue

        storage_dir = f"{self._storage_dir}/{client_id}"
        reader = storage.read_all_files(storage_dir)

        for record in reader:
            logging.debug(f"RECORD READ: {record}")
            record.insert(0, client_id)
            self.__send_record_to_forwarding_queues(record)

        self.__send_last_batch_to_forwarding_queues()
        self.__send_to_forwarding_queues(
            prefix_queue_name=queue_name,
            client_id=client_id,
            message=END_TRANSMISSION_MESSAGE,
        )

        self._clear_client_data(client_id, storage_dir)

    def __send_record_to_forwarding_queues(self, record: List[str]):
        for queue_number in range(self._amount_of_forwarding_queues):
            full_queue_name = f"{queue_number}_{self._publish_queue}"

            logging.debug(f"Sending record: {record} to queue: {full_queue_name}")

            self._middleware.publish(record, full_queue_name)

    def __send_last_batch_to_forwarding_queues(self):
        for queue_number in range(self._amount_of_forwarding_queues):
            full_queue_name = f"{queue_number}_{self._publish_queue}"

            logging.debug(f"Sending last batch to queue: {full_queue_name}")

            self._middleware.publish_batch(full_queue_name)

    def __send_to_forwarding_queues(self, prefix_queue_name, client_id, message):
        for i in range(self._amount_of_forwarding_queues):
            self._middleware.publish_message(
                [client_id, message],
                f"{i}_{prefix_queue_name}",
            )
            logging.debug(f"Sent {message} of client: {client_id}")

    def _clear_client_data(self, client_id, storage_dir):
        if not storage.delete_directory(storage_dir):
            logging.debug(f"Couldn't delete directory: {storage_dir}")
        else:
            logging.debug(f"Deleted directory: {storage_dir}")

        try:
            self._ends_received_per_client.pop(client_id)
        except KeyError:
            # When can this happen? when the timeout comes before the client data (for whatever reason)
            logging.debug(
                f"No session found with id: {client_id} while removing ends. Omitting."
            )

        try:
            self.__total_timeouts_received_per_client.pop(client_id)
        except KeyError:
            # When can this happen? when the there was no timeout for a given client
            logging.debug(
                f"No session found with id: {client_id} while removing timeouts. Omitting."
            )

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")

        self._got_sigterm = True
        # self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
        self._client_monitor.stop()
