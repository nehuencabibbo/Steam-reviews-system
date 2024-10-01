import csv
import time
import signal
import logging
from common.rabbit_connection import *
from pika.exceptions import ChannelWrongStateError

FILE_END_MSG = "END"

class Client:

    def __init__(self, config):
        self.config = config
        self.connection = RabbitConnection()
        self.__declare_queues()
        self.got_sigterm = False
        signal.signal(signal.SIGTERM, self.__sigterm_handler)


    def __declare_queues(self):

        #declare queues for sending data
        self.connection.declare_queue(self.config["GAMES_QUEUE"])
        self.connection.declare_queue(self.config["REVIEWS_QUEUE"])

        # declare consumer queues
        self.connection.declare_queue(self.config["Q1_RESULT_QUEUE"])
        self.connection.declare_queue(self.config["Q2_RESULT_QUEUE"])
        self.connection.declare_queue(self.config["Q3_RESULT_QUEUE"])
        self.connection.declare_queue(self.config["Q4_RESULT_QUEUE"])
        self.connection.declare_queue(self.config["Q5_RESULT_QUEUE"])


    def run(self):

        self.__send_file(self.config["GAMES_QUEUE"], self.config["GAME_FILE_PATH"])

        self.__send_file(self.config["REVIEWS_QUEUE"], self.config["REVIEWS_FILE_PATH"])

        self.__get_results()

        if not self.got_sigterm:
            self.connection.close()


    def __send_file(self, queue_name, file_path):

        with open(file_path, 'r') as file:
            reader = csv.reader(file) #using csv reader for logging
            next(reader, None)  #skip header
            for row in reader:

                if self.got_sigterm:
                    return
                logging.debug(f"Sending appID {row[0]} to {queue_name}") 
                msg = ','.join(row) 

                self.connection.send(queue_name, msg)
                time.sleep(self.config["SENDING_WAIT_TIME"])
        
        logging.debug("Sending file end")
        self.connection.send(queue_name, FILE_END_MSG)


    def __get_results(self):

        logging.debug("Waiting for results")
        try: 
            self.connection.consume_from_queue(self.config["Q1_RESULT_QUEUE"], self.__handle_query_result)
            self.connection.consume_from_queue(self.config["Q2_RESULT_QUEUE"], self.__handle_query_result)
            self.connection.consume_from_queue(self.config["Q3_RESULT_QUEUE"], self.__handle_query_result)
            self.connection.consume_from_queue(self.config["Q4_RESULT_QUEUE"], self.__handle_query_result)
            self.connection.consume_from_queue(self.config["Q5_RESULT_QUEUE"], self.__handle_query_result)
        except (OSError, ChannelWrongStateError) as _:
            if not self.got_sigterm:
                raise


    def __handle_query_result(self, ch, method, properties, body):

        msg = body.decode('utf-8').strip()
        self.connection.ack(method.delivery_tag)

        if msg == FILE_END_MSG:
            self.connection.stop_consuming()
            return

        logging.info(f"{method.routing_key} result: {msg}")
        

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self.got_sigterm = True
        self.connection.close()
