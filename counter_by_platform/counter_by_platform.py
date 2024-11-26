import signal
import logging
import uuid
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from typing import *
from common.watchdog_client.watchdog_client import WatchdogClient
import threading

END_TRANSMISSION_MESSAGE = "END"
SESSION_TIMEOUT_MESSAGE = "TIMEOUT"

END_TRANSMISSION_MESSAGE_INDEX = 2
END_TRANSMISSION_SESSION_ID = 0

SESSION_TIMEOUT_MESSAGE_INDEX = 1
TIMEOUT_TRANSMISSION_SESSION_ID = 0

REGULAR_MESSAGE_SESSION_ID = 0
REGULAR_MESSAGE_FIELD_TO_COUNT_BY = 2


class CounterByPlatform:

    def __init__(self, config, middleware: Middleware, monitor: WatchdogClient):
        self._middleware = middleware
        self._got_sigterm = False
        self._count_dict = {}
        self._client_monitor = monitor

        # Assigning config values to instance attributes
        self.node_id = config["NODE_ID"]
        self.consume_queue_suffix = config["CONSUME_QUEUE_SUFIX"]
        self.publish_queue = config["PUBLISH_QUEUE"]
        self.storage_dir = config["STORAGE_DIR"]

        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):

        monitor_thread = threading.Thread(target=self._client_monitor.start)
        monitor_thread.start()

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
            monitor_thread.join()

    def __handle_message(self, delivery_tag: int, body: List[List[str]]):
        body = self._middleware.get_rows_from_message(body)

        logging.debug(f"GOT BATCH: {body}")
        if body[0][SESSION_TIMEOUT_MESSAGE_INDEX] == SESSION_TIMEOUT_MESSAGE:
            session_id = body[0][TIMEOUT_TRANSMISSION_SESSION_ID]
            logging.info(f"Received timeout for client: {session_id}")

            client_dir = f"{self.storage_dir}/{session_id}"
            storage.delete_directory(client_dir)
            self._middleware.ack(delivery_tag)

            return

        if (
            len(body[0]) > 1
            and body[0][END_TRANSMISSION_MESSAGE_INDEX] == END_TRANSMISSION_MESSAGE
        ):
            logging.debug("Recived END transmssion")
            session_id = body[0][END_TRANSMISSION_SESSION_ID]

            self.__send_results(session_id)
            self._middleware.ack(delivery_tag)

            return

        count_per_record_by_client_id = self.__count_per_client_id(body)

        storage.sum_platform_batch_to_records_per_client(
            self.storage_dir,
            count_per_record_by_client_id,
        )

        self._middleware.ack(delivery_tag)

    def __count_per_client_id(self, body: list[list]):
        count_per_record_by_client_id = {}
        for record in body:
            client_id = record[REGULAR_MESSAGE_SESSION_ID]
            record_id = record[REGULAR_MESSAGE_FIELD_TO_COUNT_BY]

            if not client_id in count_per_record_by_client_id:
                count_per_record_by_client_id[client_id] = {}

            count_per_record_by_client_id[client_id][record_id] = (
                count_per_record_by_client_id[client_id].get(record_id, 0) + 1
            )

        return count_per_record_by_client_id

    def __send_results(self, session_id: str):
        self._middleware.create_queue(self.publish_queue)

        client_dir = f"{self.storage_dir}/{session_id}"
        reader = storage.read_all_files(client_dir)

        for record in reader:
            logging.debug(f"sending record: {record}")
            if self._got_sigterm:
                # should send everything so i can ack before closing
                # or return false so end is not acked and i dont send the results?
                return

            self._middleware.publish(
                [session_id, record[0], record[1]], queue_name=self.publish_queue
            )

        self._middleware.publish_batch(self.publish_queue)

        self._middleware.send_end(
            queue=self.publish_queue,
            end_message=[session_id, str(uuid.uuid4()), END_TRANSMISSION_MESSAGE],
        )
        logging.debug(f"Sent results to queue {self.publish_queue}")

        storage.delete_directory(client_dir)

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        # self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
        self._client_monitor.stop()
