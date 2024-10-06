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
        self.tmp_list = []
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
            self.send_results()
            self.middleware.ack(method.delivery_tag)
            return
        
        self.tmp_list.append(body)

        self.amount_msg_received += 1
        if (self.amount_msg_received % self.config["SAVE_AFTER_MESSAGES"]) == 0:
            logging.debug(f"Pesisting data in temporary storage")
            self.persist_data() 
        
        self.middleware.ack(method.delivery_tag)

    def persist_data(self):

        storage.save_data_batch(self.config["STORAGE_DIR"], FILE_NAME, self.tmp_list)
        self.tmp_list = [] 

    def send_results(self):
        
        k = (self.config["PERCENTILE"] / 100) * self.amount_msg_received
        
        k = round(k)
        if k == 0:
            encoded_message = self.protocol.encode(END_TRANSMISSION_MESSAGE)
            self.middleware.publish(encoded_message, self.config["PUBLISH_QUEUE"])
            return

        self.amount_msg_received = 0

        file_path = os.path.join(self.config["STORAGE_DIR"], FILE_NAME)
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            for line in reader:
                record = ','.join(line) 
                storage.add_to_top(self.config["STORAGE_DIR"], record, k)

        #if percentile is x, i need the top (100-x)% of the apps
        # read the file and use a max heap of k elements
        # the top k, are the items in the asked percentile
        
        #TODO: replace top_k with saving sorted data
        top_reader = storage.read_top(self.config["STORAGE_DIR"], k)
        for line in top_reader:

            message = line[0].split(",")
            logging.debug(f"LEO LINEA: {message}")
            encoded_message = self.protocol.encode(message)
            self.middleware.publish(encoded_message, self.config["PUBLISH_QUEUE"])

        encoded_message = self.protocol.encode(END_TRANSMISSION_MESSAGE)
        self.middleware.publish(encoded_message, self.config["PUBLISH_QUEUE"])

        self.amount_msg_received = 0


    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self.got_sigterm = True
        self.middleware.shutdown()

