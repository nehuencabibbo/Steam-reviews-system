import pika
import csv
import time
import signal
import logging

FILE_END_MSG = "END"

class Client:

    def __init__(self, config):
        self.config = config
        
        self.got_sigterm = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)


    def run(self):

        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        channel.queue_declare(queue=self.config["GAMES_QUEUE"], durable=True)
        channel.queue_declare(queue=self.config["REVIEWS_QUEUE"], durable=True)

        self.send_file(channel, self.config["GAMES_QUEUE"], self.config["GAME_FILE_PATH"])

        self.send_file(channel, self.config["REVIEWS_QUEUE"], self.config["REVIEWS_FILE_PATH"])

        connection.close()


    def send_file(self, channel, queue_name, file_path):

        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader, None)  #omit header
            for row in reader:

                if self.got_sigterm:
                    return
                logging.debug(f"Sending appID {row[0]} to {queue_name}")

                msg = ','.join(row)
                channel.basic_publish(exchange='', routing_key=queue_name, body=msg)
                time.sleep(self.config["SENDING_WAIT_TIME"])
        
        logging.debug("Sending file end")
        channel.basic_publish(exchange='', routing_key=queue_name, body=FILE_END_MSG)


    def sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self.got_sigterm = True