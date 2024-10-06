import signal
import logging
from common.middleware.middleware import Middleware
from common.storage import storage
from common.protocol.protocol import Protocol

import os
import csv
import heapq

END_TRANSMISSION_MESSAGE = ['END'] 

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
        
        record = ",".join(body)
        self.tmp_list.append(record)

        self.amount_msg_received += 1
        if (self.amount_msg_received % self.config["SAVE_AFTER_MESSAGES"]) == 0:
            logging.debug(f"Pesisting data in temporary storage")
            self.persist_data() 
        
        self.middleware.ack(method.delivery_tag)

    def persist_data(self):

        for record in self.tmp_list:
            storage.write_by_range( 
                self.config["STORAGE_DIR"], 
                self.config["RANGE_FOR_PARTITION"],
                record)    
            
        self.tmp_list = []

    def send_results(self):
        
        reader = storage.read_by_range(self.config["STORAGE_DIR"], 1, 0)
        k = (100 - self.config["PERCENTIL"]) * self.amount_msg_received
        #if percentile is 90, i need the top 10% of the app_ids
        #read the file and use a max heap of k elements
        # the top k, are the items in the asked percentile

        for line in reader:
            logging.debug(f"LEO LINEA: {line}")
            encoded_message = self.protocol.encode(line)
            self.middleware.publish(encoded_message, self.config["PUBLISH_QUEUE"])

        encoded_message = self.protocol.encode(END_TRANSMISSION_MESSAGE)
        self.middleware.publish(encoded_message, self.config["PUBLISH_QUEUE"])


    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self.got_sigterm = True
        self.middleware.shutdown()

