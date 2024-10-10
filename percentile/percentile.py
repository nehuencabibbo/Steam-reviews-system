import signal
import logging
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from common.protocol.protocol import Protocol
import math

END_TRANSMISSION_MESSAGE = ['END'] 
FILE_NAME = "percentile_data.csv"

class Percentile:

    def __init__(self, config:dict, middleware:Middleware, protocol: Protocol):
        self._protocol = protocol
        self._config = config
        self._middleware = middleware
        self._got_sigterm = False
        self._tmp_record_list = []
        self._amount_msg_received = 0
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):

        self._middleware.create_queue(self._config["CONSUME_QUEUE"])
        self._middleware.create_queue(self._config["PUBLISH_QUEUE"])

        self._middleware.attach_callback(self._config['CONSUME_QUEUE'], self.handle_message)
        
        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)
    
    def handle_message(self, ch, method, properties, body):
        # body = self._protocol.decode(body)
        # body = [value.strip() for value in body]

        body = self._middleware.get_rows_from_message(body)

        for message in body:
            logging.debug(f"GOT MSG: {message}")

            if len(message) == 1 and message[0] == "END":
                self.persist_data() #persist data that was not saved
                self.handle_end_message()
                self._middleware.ack(method.delivery_tag)
                return
            
            message = ",".join(message)
            self._tmp_record_list.append(message)

            self._amount_msg_received += 1
            if (self._amount_msg_received % self._config["SAVE_AFTER_MESSAGES"]) == 0:
                logging.debug(f"Pesisting data in temporary storage")
                self.persist_data() 
            
        self._middleware.ack(method.delivery_tag)

    def persist_data(self):

        for record in  self._tmp_record_list:
            storage.add_to_sorted_file(self._config["STORAGE_DIR"], record)
        self._tmp_record_list = [] 

    def handle_end_message(self):
        
        rank = (self._config["PERCENTILE"] / 100) * self._amount_msg_received
        rank = math.ceil(rank) # instead of interpolating, round the number

        logging.debug(f"Ordinal rank is {rank}")

        percentile = self.get_percentile()

        reader = storage.read_sorted_file(self._config["STORAGE_DIR"])
        for row in reader:
            record = row[0].split(",")
            record_value = int(record[1])
            if record_value >= percentile:
                logging.debug(f"Sending: {record}")
                #encoded_message = self._protocol.encode(record)
                self._middleware.publish(record, self._config["PUBLISH_QUEUE"])

        self._middleware.publish_batch(self._config["PUBLISH_QUEUE"])
        self._middleware.send_end(self._config["PUBLISH_QUEUE"])

        # encoded_message = self._protocol.encode(END_TRANSMISSION_MESSAGE)
        # self._middleware.publish(encoded_message, self._config["PUBLISH_QUEUE"])

        self._amount_msg_received = 0
        # if not storage.delete_directory(self._config["STORAGE_DIR"]):
        #     logging.debug(f"Couldn't delete directory: {self._config["STORAGE_DIR"]}")
        # else: 
        #     logging.debug(f"Deleted directory: {self._config["STORAGE_DIR"]}")

    def get_percentile(self):
        rank = (self._config["PERCENTILE"] / 100) * self._amount_msg_received
        rank = round(rank)

        logging.debug(f"Ordinal rank is {rank}")

        reader = storage.read_sorted_file(self._config["STORAGE_DIR"])
        for i, row in enumerate(reader):
            if i == rank:
                _, value = row[0].split(",")
                return int(value)

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        self._middleware.shutdown()

