import os
import signal
import logging
from typing import *
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from common.activity_log.activity_log import ActivityLog
import math

from utils.utils import group_batch_by_field

END_TRANSMISSION_MESSAGE = "END"
FILE_NAME = "percentile_data.csv"
NO_RECORDS = 0


class Percentile:

    def __init__(self, config: dict, middleware: Middleware, activity_log: ActivityLog):
        self._middleware = middleware
        self._got_sigterm = False
        self._recived_ends = {}
        self._consume_queue = config["CONSUME_QUEUE"]
        self._publish_queue = config["PUBLISH_QUEUE"]
        self._needed_ends_to_finish = config["NEEDED_ENDS_TO_FINISH"]
        self._storage_dir = config["STORAGE_DIR"]
        self._percentile = config["PERCENTILE"]

        self._activity_log = activity_log
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

        self.__recover_state()

    def run(self):

        self._middleware.create_queue(self._consume_queue)
        self._middleware.create_queue(self._publish_queue)

        callback = self._middleware.generate_callback(self._handle_message)
        self._middleware.attach_callback(self._consume_queue, callback)

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self._middleware.shutdown()

    def __recover_state(self):
        full_file_path, lines = self._activity_log.recover()

        if not full_file_path or not lines:
            logging.debug("General log was corrupted, not recovering any state.")
            return

        logging.debug(
            f"Recovering state, {full_file_path} is being processed with batch: "
        )
        for line in lines:
            logging.debug(line)
        dir = full_file_path.rsplit("/", maxsplit=1)[0]

        if not os.path.exists(dir):
            logging.debug(
                (
                    f"Ended up aborting state recovery, as {dir} "
                    f"was cleaned up after receiving END"
                )
            )
            return

        lines = list(map(lambda x: x.rsplit(",", maxsplit=2), lines))
        logging.debug(f"LINEAS: {lines}")
        storage._add_batch_to_sorted_file(
            dir, 
            lines, 
            self._activity_log, 
        )

    def __purge_duplicates(self, records_per_client: Dict[str, List[List[str]]]):
        # Para cada batch, para cada cliente, se actualiza por completo o no un determinado archivo (firma de
        # la func del storage), si al menos UN msg_id para un cliente ya fue procesado, eso quiere decir que
        # todos los fueron, por lo tanto no hace falta checkear todos

        client_ids_to_remove = []
        for client_id, records in records_per_client.items():
            for msg_id, name, count in records:  # TODO: Por ahi hay alguna forma mejor de hacer esto
                if self._activity_log.is_msg_id_already_processed(client_id, msg_id):
                    client_ids_to_remove.append(client_id)

                break

        for client_id in client_ids_to_remove:
            del records_per_client[client_id]

    def _handle_message(self, delivery_tag, body):

        body = self._middleware.get_rows_from_message(body)
        logging.debug(f"BODY: {body}")

        if len(body) == 1 and body[0][1] == END_TRANSMISSION_MESSAGE:
            client_id = body[0][0]
            logging.debug(f'END FROM CLIENT: {client_id}')
            self._recived_ends[client_id] = self._recived_ends.get(client_id, 0) + 1

            logging.debug(f"GOT END NUMBER: {self._recived_ends[client_id]}")

            if self._recived_ends[client_id] == self._needed_ends_to_finish:
                # create queue for answering
                # self._middleware.create_queue(
                #     f'{self._config["PUBLISH_QUEUE"]}_{client_id}'
                # )
                self._handle_end_message(client_id)

            self._middleware.ack(delivery_tag)
            return

        records_per_client = group_batch_by_field(body)
        self.__purge_duplicates(records_per_client)

        storage.add_batch_to_sorted_file_per_client(self._storage_dir, records_per_client, self._activity_log)

        self._middleware.ack(delivery_tag)
        

    def _handle_end_message(self, client_id):

        percentile = self._get_percentile(client_id)
        logging.debug(f"Percentile is: {percentile}")

        forwarding_queue_name = self._publish_queue
        storage_dir = f"{self._storage_dir}/{client_id}"

        reader = storage.read_sorted_file(storage_dir)
        for row in reader:

            record_value = int(row[1])
            if record_value >= percentile:
                logging.debug(f"Sending: {row}")
                row.insert(0, client_id)
                self._middleware.publish(row, forwarding_queue_name)

        self._middleware.publish_batch(forwarding_queue_name)
        self._middleware.send_end(
            forwarding_queue_name, end_message=[client_id, END_TRANSMISSION_MESSAGE]
        )

        self._clear_client_data(client_id, storage_dir)

    def _get_percentile(self, client_id):
        # to get the rank, i need to read the file if i do not have a countera (i need the amount of messages)
        rank = self._get_rank(client_id)

        logging.debug(f"Ordinal rank is {rank}")

        storage_dir = f"{self._storage_dir}/{client_id}"
        reader = storage.read_sorted_file(storage_dir)
        for i, row in enumerate(reader):
            if (i + 1) == rank:
                name, value, msg_id = row
                logging.debug(f"VALUE: {value}")
                return int(value)

        return NO_RECORDS

    def _get_rank(self, client_id):
        amount_of_records = 0

        storage_dir = f"{self._storage_dir}/{client_id}"
        reader = storage.read_sorted_file(storage_dir)
        for _ in reader:
            amount_of_records += 1

        rank = (self._percentile / 100) * amount_of_records
        rank = math.ceil(rank)  # instead of interpolating, round the number

        return rank

    def _clear_client_data(self, client_id: str, storage_dir: str):

        if not storage.delete_directory(storage_dir):
            logging.debug(f"Couldn't delete directory: {storage_dir}")
        else:
            logging.debug(f"Deleted directory: {storage_dir}")
        self._recived_ends.pop(client_id)  # removed end count for the client

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        # self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
