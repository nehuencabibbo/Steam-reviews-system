import sys
import csv
import time
import signal
import logging
from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol

FILE_END_MSG = "END"
AMMOUNT_OF_QUERIES = 5

class Client:

    def __init__(self, config:dict, middleware:Middleware, protocol:Protocol):
        self.protocol = protocol
        self.config = config
        self.middleware = middleware
        self.__create_queues()
        self.got_sigterm = False

        self.__find_and_set_csv_field_size_limit()
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    @staticmethod
    def __find_and_set_csv_field_size_limit():
        '''
        Taken from:
        https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
        '''
        max_int = 4294967296 # 2^32 - 1 -> Max size supported by our protocol
        while True:
            try:
                csv.field_size_limit(max_int)  
                break  
            except OverflowError:
                max_int = int(max_int / 10) 

    def __create_queues(self):

        #declare queues for sending data
        self.middleware.create_queue(self.config["GAMES_QUEUE"])
        self.middleware.create_queue(self.config["REVIEWS_QUEUE"])

        # declare consumer queues
        for i in range(1, AMMOUNT_OF_QUERIES + 1):
            queue_name = f"Q{i}_RESULT_QUEUE"
            self.middleware.create_queue(self.config[queue_name])


    def run(self):

        self.__send_file(self.config["GAMES_QUEUE"], self.config["GAME_FILE_PATH"])
        # self.__send_file(self.config["REVIEWS_QUEUE"], self.config["REVIEWS_FILE_PATH"])

        self.__get_results()

        if not self.got_sigterm:
            self.middleware.shutdown()


    def __send_file(self, queue_name, file_path):
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader, None)  #skip header
            for row in reader:

                if self.got_sigterm:
                    return
                logging.debug(f"Sending appID {row[0]} to {queue_name}") 
                encoded_message = self.protocol.encode(row)

                self.middleware.publish(encoded_message, queue_name=queue_name)
                time.sleep(self.config["SENDING_WAIT_TIME"])
        
        logging.debug("Sending file end")
        encoded_message = self.protocol.encode([FILE_END_MSG])
        self.middleware.publish(encoded_message, queue_name=queue_name)


    def __get_results(self):

        for number_of_query in range(1, AMMOUNT_OF_QUERIES + 1):
            if self.got_sigterm:
                return
            logging.debug(f"Waiting for results of query {number_of_query}")

            queue_name = self.config[f"Q{number_of_query}_RESULT_QUEUE"]

            self.middleware.attach_callback(queue_name, self.__handle_query_result)
            try:
                self.middleware.start_consuming()
            except OSError as _:
                if not self.got_sigterm: raise


    def __handle_query_result(self, ch, method, properties, body):

        body = Protocol.decode(body)
        body = [value.strip() for value in body]

        self.middleware.ack(method.delivery_tag)

        if len(body) == 1 and body[0] == FILE_END_MSG:
            self.middleware.stop_consuming()
            return
        
        logging.info(f"{method.routing_key} result: {body}")
        

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self.got_sigterm = True
        self.middleware.shutdown()
