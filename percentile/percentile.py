import os
import signal
import logging
import time
from typing import *
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from common.activity_log.activity_log import ActivityLog
import math

FILE_NAME = "percentile_data.csv"
NO_RECORDS = 0

END_TRANSMISSION_MESSAGE = "END"
END_TRANSMISSION_MESSAGE_CLIENT_ID_INDEX = 0
END_TRANSMISSION_MESSAGE_MSG_ID_INDEX = 1
END_TRANSMISSION_MESSAGE_END_INDEX = 2

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
        self._node_id = config["NODE_ID"]

        self._activity_log = activity_log

        self._recived_ends = self._activity_log.recover_ends_state()

        signal.signal(signal.SIGTERM, self.__sigterm_handler)


    def run(self):

        self._middleware.create_queue(self._consume_queue)
        self._middleware.create_queue(self._publish_queue)

        callback = self._middleware.generate_callback(self._handle_message)
        self._middleware.attach_callback(self._consume_queue, callback)

        self.__resume_publish_if_necesary()
        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self._middleware.shutdown()

        
    def __resume_publish_if_necesary(self):
        client_ids_to_remove = []
        for client_id, amount in self._recived_ends.items():
            if amount == self._needed_ends_to_finish:
                self._send_results(client_id)
                client_ids_to_remove.append(client_id)

        for client_id in client_ids_to_remove:
            client_dir = os.path.join(self._storage_dir, client_id)
            self._clear_client_data(client_id,client_dir)
            self._activity_log.remove_client_logs(client_id)


    def _handle_message(self, delivery_tag, body):

        body = self._middleware.get_rows_from_message(body)
        logging.debug(f"BODY: {body}")

        if len(body) == 1 and body[0][END_TRANSMISSION_MESSAGE_END_INDEX] == END_TRANSMISSION_MESSAGE:
            client_id = body[0][END_TRANSMISSION_MESSAGE_CLIENT_ID_INDEX]
            msg_id = body[0][END_TRANSMISSION_MESSAGE_MSG_ID_INDEX]

            self._handle_end_transmission(client_id, msg_id)

            self._middleware.ack(delivery_tag)

            return

        storage.add_batch_to_sorted_file_per_client(
            self._storage_dir, 
            body
        )
        
        self._middleware.ack(delivery_tag)


    def _handle_end_transmission(self, client_id: str, msg_id: str):
        logging.debug(f'END FROM CLIENT: {client_id}')

        client_dir = os.path.join(self._storage_dir, client_id)
        if not os.path.exists(client_dir):
            logging.debug('Recived END, but client directory was already deleted. Probably duplicate')

            self._middleware.send_end(
                self._publish_queue, 
                end_message=[client_id, self._node_id, END_TRANSMISSION_MESSAGE]
            )
            return
        
        was_duplicate = self._activity_log.log_end(client_id, msg_id)
        if was_duplicate: 
            logging.debug(f'Filtered duplicate END {msg_id}')

            return 

        self._recived_ends[client_id] = self._recived_ends.get(client_id, 0) + 1

        logging.debug(f"GOT END NUMBER: {self._recived_ends[client_id]}")

        if self._recived_ends[client_id] == self._needed_ends_to_finish:
            self._send_results(client_id)
            self._clear_client_data(client_id, client_dir)
            self._activity_log.remove_client_logs(client_id)
        

    def _send_results(self, client_id):

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
            forwarding_queue_name, 
            end_message=[client_id, self._node_id, END_TRANSMISSION_MESSAGE]
        )

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
        
        if client_id in self._recived_ends:
            del self._recived_ends[client_id]

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        # self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
