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
        self._recived_ends = 0
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):

        self._middleware.create_queue(self._config["CONSUME_QUEUE"])
        self._middleware.create_queue(self._config["PUBLISH_QUEUE"])

        self._middleware.attach_callback(
            self._config["CONSUME_QUEUE"], self._handle_message
        )

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)

    def _handle_message(self, ch, method, properties, body):

        body = self._middleware.get_rows_from_message(body)
        logging.debug(f"body: {body}")

        if len(body) == 1 and body[0][1] == END_TRANSMISSION_MESSAGE:
            self._recived_ends += 1 # use hash when using multiple clients
            logging.debug(f"GOT END NUMBER: {self._recived_ends}")
            
            if self._recived_ends == self._config["NEEDED_ENDS_TO_FINISH"]:
                client_id = body[0][0]
                #create queue for answering
                self._middleware.create_queue(f'{self._config["PUBLISH_QUEUE"]}_{client_id}')
                self._handle_end_message(client_id)

            self._middleware.ack(method.delivery_tag)
            return

        storage.add_batch_to_sorted_file_per_client(self._config["STORAGE_DIR"], body)
        # records_per_client = {}
        # for record in body:
        #     client_id = record.pop(0)

        #     if not client_id in records_per_client:
        #         records_per_client[client_id] = []
            
        #     records_per_client[client_id].append(record)
        
        # logging.debug(f"RECORD PER CLIENT: {records_per_client}")

        # #TODO: make storage handle batches from multiple clients
        # for client_id, records in records_per_client.items():
        #     storage_dir = f'{self._config["STORAGE_DIR"]}/{client_id}'
        #     storage.add_batch_to_sorted_file(storage_dir, records)

        self._middleware.ack(method.delivery_tag)

    def _handle_end_message(self, client_id):

        percentile = self._get_percentile(client_id)
        logging.info(f"Percentile is: {percentile}")

        forwarding_queue_name = f'{self._config["PUBLISH_QUEUE"]}_{client_id}'
        storage_dir = f'{self._config["STORAGE_DIR"]}/{client_id}'

        reader = storage.read_sorted_file(storage_dir)
        for row in reader:

            record_value = int(row[1])
            if record_value >= percentile:
                logging.debug(f"Sending: {row}")
                self._middleware.publish(row, forwarding_queue_name)

        self._middleware.publish_batch(forwarding_queue_name)
        self._middleware.send_end(forwarding_queue_name)

        #self._amount_msg_received = 0
        # if not storage.delete_directory(self._config["STORAGE_DIR"]):
        #     logging.debug(f"Couldn't delete directory: {self._config["STORAGE_DIR"]}")
        # else:
        #     logging.debug(f"Deleted directory: {self._config["STORAGE_DIR"]}")

    def _get_percentile(self, client_id):
        # to get the rank, i need to read the file if i do not have a countera (i need the amount of messages)
        rank = self._get_rank(client_id)

        logging.debug(f"Ordinal rank is {rank}")

        storage_dir = f'{self._config["STORAGE_DIR"]}/{client_id}'
        reader = storage.read_sorted_file(storage_dir)
        for i, row in enumerate(reader):
            if (i + 1) == rank:
                _, value = row
                logging.debug(f"VALUE: {value}")
                return int(value)

        return NO_RECORDS
    
    def _get_rank(self, client_id):
        amount_of_records = 0

        storage_dir = f'{self._config["STORAGE_DIR"]}/{client_id}'
        reader = storage.read_sorted_file(storage_dir)
        for _ in reader:
           amount_of_records += 1

        rank = (self._config["PERCENTILE"] / 100) * amount_of_records
        rank = math.ceil(rank)  # instead of interpolating, round the number

        return rank

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        self._middleware.shutdown()
