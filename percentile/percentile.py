import signal
import logging
from common.middleware.middleware import Middleware
from common.storage import storage
from common.protocol.protocol import Protocol

import os
import csv

END_TRANSMISSION_MESSAGE = ['END'] 
FILE_NAME = "percentile_data.csv"

class Percentile:

    def __init__(self, config:dict, middleware:Middleware, protocol: Protocol):
        self.protocol = protocol
        self.config = config
        self.middleware = middleware
        self.got_sigterm = False
        self.tmp_record_list = []
        self.amount_msg_received = 0
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):

        self.middleware.create_queue(self.config["CONSUME_QUEUE"])
        self.middleware.create_queue(self.config["PUBLISH_QUEUE"])

        self.middleware.attach_callback(self.config['CONSUME_QUEUE'], self.handle_message)
        
        try:
            logging.debug("Starting to consume...")
            self.middleware.start_consuming()
        except OSError as _:
            if not self.got_sigterm: raise
    
    def handle_message(self, ch, method, properties, body):
        body = self.protocol.decode(body)
        body = [value.strip() for value in body]

        logging.debug(f"GOT MSG: {body}")

        if len(body) == 1 and body[0] == "END":
            self.persist_data() #persist data that was not saved
            self.handle_end_message()
            self.middleware.ack(method.delivery_tag)
            return
        
        body = ",".join(body)
        self.tmp_record_list.append(body)

        self.amount_msg_received += 1
        if (self.amount_msg_received % self.config["SAVE_AFTER_MESSAGES"]) == 0:
            logging.debug(f"Pesisting data in temporary storage")
            self.persist_data() 
        
        self.middleware.ack(method.delivery_tag)

    def persist_data(self):

        for record in  self.tmp_record_list:
            storage.add_to_sorted_file(self.config["STORAGE_DIR"], record)
        self.tmp_record_list = [] 

    def handle_end_message(self):
        
        rank = (self.config["PERCENTILE"] / 100) * self.amount_msg_received
        rank = round(rank) # instead of interpolating, round the number

        logging.debug(f"Ordinal rank is {rank}")

        percentile = self.get_percentile()

        reader = storage.read_sorted_file(self.config["STORAGE_DIR"])
        for row in reader:
            record = row[0].split(",")
            record_value = int(record[1])
            if record_value >= percentile:
                logging.debug(f"Sending: {record}")
                encoded_message = self.protocol.encode(record)
                self.middleware.publish(encoded_message, self.config["PUBLISH_QUEUE"])

        encoded_message = self.protocol.encode(END_TRANSMISSION_MESSAGE)
        self.middleware.publish(encoded_message, self.config["PUBLISH_QUEUE"])

        self.amount_msg_received = 0

    def get_percentile(self):
        rank = (self.config["PERCENTILE"] / 100) * self.amount_msg_received
        rank = round(rank)

        logging.debug(f"Ordinal rank is {rank}")

        reader = storage.read_sorted_file(self.config["STORAGE_DIR"])
        for i, row in enumerate(reader):
            if i == rank:
                _, value = row[0].split(",")
                return int(value)

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self.got_sigterm = True
        self.middleware.shutdown()

