import csv
import os
import signal
import logging
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from common.watchdog_client.watchdog_client import WatchdogClient
import threading
from common.protocol.protocol import Protocol
from common.activity_log.activity_log import ActivityLog
from typing import *
from utils.utils import group_msg_ids_per_client_by_field

END_TRANSMISSION_MESSAGE = "END"
SESSION_TIMEOUT_MESSAGE = "TIMEOUT"

END_TRANSMISSION_MSG_ID_INDEX = 1
END_TRANSMISSION_CLIENT_ID = 0
END_TRANSMISSION_END_INDEX = 2

SESSION_TIMEOUT_MESSAGE_INDEX = 1
TIMEOUT_TRANSMISSION_SESSION_ID = 0

REGULAR_MESSAGE_CLIENT_ID = 0
REGULAR_MESSAGE_MSG_ID = 1
REGULAR_MESSAGE_FIELD_TO_COUNT_BY = 2


class CounterByPlatform:

    def __init__(
        self,
        config,
        middleware: Middleware,
        monitor: WatchdogClient,
        activity_log: ActivityLog,
    ):
        self._middleware = middleware
        self._got_sigterm = False
        self._count_dict = {}
        self._activity_log = activity_log
        self._client_monitor = monitor
        # Assigning config values to instance attributes
        self.node_id = config["NODE_ID"]
        self.consume_queue_suffix = config["CONSUME_QUEUE_SUFIX"]
        self.publish_queue = config["PUBLISH_QUEUE"]
        self.storage_dir = config["STORAGE_DIR"]

        signal.signal(signal.SIGTERM, self.__sigterm_handler)

        self.__recover_state()

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

    def __handle_message(self, delivery_tag: int, body: List[List[str]]):
        body = self._middleware.get_rows_from_message(body)
        
        logging.debug(  '-----------------------')
        for msg in body:
            logging.debug(f'{msg[0]}, {msg[1]}, {msg[2]}')
        if body[0][SESSION_TIMEOUT_MESSAGE_INDEX] == SESSION_TIMEOUT_MESSAGE:
            session_id = body[0][TIMEOUT_TRANSMISSION_SESSION_ID]
            logging.info(f"Received timeout for client: {session_id}")

            client_dir = f"{self.storage_dir}/{session_id}"
            # storage.delete_directory(client_dir)
            self._middleware.ack(delivery_tag)

            return

        if body[0][END_TRANSMISSION_END_INDEX] == END_TRANSMISSION_MESSAGE:
            logging.debug(f"Recived END transmssion from client: {body[0][END_TRANSMISSION_CLIENT_ID]}")
            session_id = body[0][END_TRANSMISSION_CLIENT_ID]
            msg_id = body[0][END_TRANSMISSION_MSG_ID_INDEX]

            self.__send_results(session_id, msg_id)

            self._middleware.ack(delivery_tag)

            return

        # {client_id: {
        #   Windows: [MSG_ID1, MSG_ID2, ...],
        #   Linux: [MSG_ID1, MSG_ID2, ...],
        #   Mac: [MSG_ID1, MSG_ID2, ...]},
        # ...}
        body = self.__purge_duplicates_and_add_unique_msg_id(body)

        storage.sum_batch_to_records_per_client(
            self.storage_dir, 
            body, 
            self._activity_log
        )

        self._middleware.ack(delivery_tag)

    def __generate_unique_msg_id(self, platform: str, msg_id: str) -> str:
        """
        Generated msg id is just the first char of platform as ascii concatenated
        to the msg id.

        This assumes that platform initial is unique for each platform.
        """
        return str(ord(platform[0])) + msg_id

    def __purge_duplicates_and_add_unique_msg_id(self, batch: List[str]) -> List[str]:
        # CADA mensaje individual me tengo que fijar si esta duplicado, incluido dentro del mismo batch
        # (se puede optimizar en el middleware y cambiarlo aca tmb dsps)
        batch_msg_ids = set()
        filtered_batch = []
        for msg in batch:
            # Add a custom msg id based on plaftorm
            platform = msg[REGULAR_MESSAGE_FIELD_TO_COUNT_BY]
            msg[REGULAR_MESSAGE_MSG_ID] = self.__generate_unique_msg_id(
                platform, msg[REGULAR_MESSAGE_MSG_ID]
            )

            msg_id = msg[REGULAR_MESSAGE_MSG_ID]
            client_id = msg[REGULAR_MESSAGE_CLIENT_ID]
            if (
                not self._activity_log.is_msg_id_already_processed(client_id, msg_id)
                and not (client_id, msg_id) in batch_msg_ids
            ):
                filtered_batch.append(msg)
                batch_msg_ids.add((client_id, msg_id))
            else:
                if msg_id in batch_msg_ids:
                    logging.debug(
                        f"Filtered {msg_id} beacause it was duplicated (inside batch)"
                    )
                else:
                    logging.debug(
                        f"Filtered {msg_id} beacause it was duplicated (previously processed)"
                    )

        return filtered_batch

    def __send_results(self, session_id: str, end_msg_id: str):
        # TODO: Hacer que si no existe el directorio, simplemente propague el end
        PLATFORM = 0
        MSG_ID = 1
        COUNT = 2
        self._middleware.create_queue(self.publish_queue)

        client_dir = f"{self.storage_dir}/{session_id}"
        reader = storage.read_all_files(client_dir)

        for record in reader:
            if self._got_sigterm:
                # should send everything so i can ack before closing
                # or return false so end is not acked and i dont send the results?
                return

            record_to_send = [session_id, record[MSG_ID], record[PLATFORM], record[COUNT]]
            logging.debug(f"sending record: {record_to_send} to {self.publish_queue}")
            self._middleware.publish(
                record_to_send,
                queue_name=self.publish_queue,
            )

        self._middleware.publish_batch(self.publish_queue)
        self._middleware.send_end(
            queue=self.publish_queue,
            end_message=[session_id, end_msg_id, END_TRANSMISSION_MESSAGE],
        )
        logging.debug(f"Sent results to queue {self.publish_queue}")

        storage.delete_directory(client_dir)

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        # self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
        self._client_monitor.stop()
