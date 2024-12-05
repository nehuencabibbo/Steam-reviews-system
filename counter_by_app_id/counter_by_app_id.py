import csv
import os
import signal
import logging
from typing import *
import threading

from common.activity_log.activity_log import ActivityLog
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from common.watchdog_client.watchdog_client import WatchdogClient
from utils.utils import node_id_to_send_to, group_msg_ids_per_client_by_field

END_TRANSMISSION_MESSAGE = "END"
SESSION_TIMEOUT_MESSAGE = "TIMEOUT"

SESSION_TIMEOUT_MESSAGE_INDEX = 1

END_TRANSMISSION_MESSAGE_CLIENT_ID_INDEX = 0
END_TRANSMISSION_MESSAGE_MSG_ID_INDEX = 1
END_TRANSMISSION_MESSAGE_END_INDEX = 2

REGULAR_MESSAGE_CLIENT_ID = 0
REGULAR_MESSAGE_MSG_ID = 1
REGULAR_MESSAGE_APP_ID = 2


class CounterByAppId:

    def __init__(
        self,
        config,
        middleware: Middleware,
        monitor: WatchdogClient,
        activity_log: ActivityLog,
    ):
        self._config = config
        self._middleware = middleware
        self._got_sigterm = False
        self._ends_received_per_client = {}
        self.__total_timeouts_received_per_client = {}
        self._client_monitor = monitor

        # Configuration attributes
        self._node_id = config["NODE_ID"]
        self._consume_queue_suffix = config["CONSUME_QUEUE_SUFIX"]
        self._amount_of_forwarding_queues = config["AMOUNT_OF_FORWARDING_QUEUES"]
        self._publish_queue = config["PUBLISH_QUEUE"]
        self._storage_dir = config["STORAGE_DIR"]
        self._range_for_partition = config["RANGE_FOR_PARTITION"]
        self._needed_ends = config["NEEDED_ENDS"]
        self._activity_log = activity_log

        signal.signal(signal.SIGTERM, self.__sigterm_handler)

        self.__recover_state()

        self._ends_received_per_client = self._activity_log.recover_ends_state()

    def _resume_publish_if_necesary(self):
        for client_id, amount in self._ends_received_per_client.items():
            if amount == self._needed_ends:
                # Internamente borra todo
                self.__send_results(client_id)

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

        self._resume_publish_if_necesary()
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
            session_id = body[0][0]  # TODO: Mover a una constante
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

        if body[0][END_TRANSMISSION_MESSAGE_END_INDEX] == END_TRANSMISSION_MESSAGE:
            client_id = body[0][END_TRANSMISSION_MESSAGE_CLIENT_ID_INDEX]
            msg_id = body[0][END_TRANSMISSION_MESSAGE_MSG_ID_INDEX]

            self.__handle_end_transmssion(client_id, msg_id)

            self._middleware.ack(delivery_tag)

            return

        body = self.__purge_duplicates(body)

        storage.sum_batch_to_records_per_client(
            self._storage_dir,
            body,
            self._activity_log,
            range_for_partition=self._range_for_partition,
            save_first_msg_id=True,
        )

        self._middleware.ack(delivery_tag)

    def __handle_end_transmssion(self, client_id: str, msg_id: str):
        was_duplicate = self._activity_log.log_end(client_id, msg_id)
        if was_duplicate:
            logging.debug((f"END {msg_id} was duplicate, filtering it out"))

            return

        self._ends_received_per_client[client_id] = (
            self._ends_received_per_client.get(client_id, 0) + 1
        )

        logging.debug(
            f"Amount of ends received up to now: {self._ends_received_per_client[client_id]} | Expecting: {self._needed_ends}"
        )
        if self._ends_received_per_client[client_id] == self._needed_ends:
            self.__send_results(client_id)

    def __recover_state(self):
        full_file_path, file_state = self._activity_log.recover()
        if not full_file_path or not file_state:
            logging.debug("General log was corrupted, not recovering any state.")
            return

        logging.debug(f"Recovering state, overriding {full_file_path} with: ")
        for line in file_state:
            logging.debug(line)

        dir, file_name = full_file_path.rsplit("/", maxsplit=1)
        if not os.path.exists(dir):
            logging.debug(
                (
                    f"Ended up aborting state recovery, as {dir} "
                    f"was cleaned up after receiving END"
                )
            )
            return

        # TODO: Encapsular en el storage esto para que no haga falta
        # saber de aca que s eusa csv
        temp_file = os.path.join(dir, f"temp_{file_name}")
        with open(temp_file, mode="w", newline="") as temp:
            writer = csv.writer(temp)
            for line in file_state:
                writer.writerow(line.split(","))

        os.replace(temp_file, full_file_path)

    def __purge_duplicates(self, batch: List[str]) -> List[str]:
        batch_msg_ids = set()
        filtered_batch = []
        for msg in batch:
            # Add a custom msg id based on plaftorm
            client_id = msg[REGULAR_MESSAGE_CLIENT_ID]
            msg_id = msg[REGULAR_MESSAGE_MSG_ID]
            app_id = msg[REGULAR_MESSAGE_APP_ID]

            if (
                not self._activity_log.is_msg_id_already_processed(client_id, msg_id)
                and not (client_id, msg_id) in batch_msg_ids
            ):
                filtered_batch.append(msg)
                batch_msg_ids.add((client_id, msg_id))
            else:
                if msg_id in batch_msg_ids:
                    logging.debug(
                        f"[DUPLICATE FILTER] Filtered {msg_id} beacause it was repeated (inside batch)"
                    )
                else:
                    logging.debug(
                        f"[DUPLICATE FILTER] Filtered {msg_id} beacause it was repeated (previously processed)"
                    )

        return filtered_batch

    def __send_results(self, client_id: str):
        storage_dir = f"{self._storage_dir}/{client_id}"
        reader = storage.read_all_files(storage_dir)

        for app_id, msg_id, value in reader:
            record = [client_id, msg_id, app_id, value]
            self.__send_record_to_forwarding_queues(record)

        self.__send_last_batch_to_forwarding_queues()

        self.__send_end_to_forwarding_queues(
            prefix_queue_name=self._publish_queue, client_id=client_id
        )
        self._clear_client_data(client_id, storage_dir)
        self._activity_log.remove_client_logs(client_id)

        # self._clear_client_data(client_id, storage_dir)

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

    def __send_end_to_forwarding_queues(self, prefix_queue_name, client_id):
        for i in range(self._amount_of_forwarding_queues):
            self._middleware.send_end(
                f"{i}_{prefix_queue_name}",
                end_message=[client_id, str(self._node_id), END_TRANSMISSION_MESSAGE],
            )
            logging.debug(f"Sent END of client: {client_id}")

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

        if client_id in self._ends_received_per_client:
            del self._ends_received_per_client[client_id]
        if client_id in self.__total_timeouts_received_per_client:
            del self.__total_timeouts_received_per_client[client_id]

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")

        self._got_sigterm = True
        # self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
        self._client_monitor.stop()
