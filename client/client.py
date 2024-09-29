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

        channel.queue_declare(queue=self.config["OUTPUT_QUEUE"], durable=True)

        self.send_file(channel, self.config["GAME_FILE_PATH"])

        connection.close()


    def send_file(self, channel, file_path):

        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader, None)  
            for row in reader:

                if self.got_sigterm:
                    return

                logging.debug(f"Sending info of app {row[0]}")

                msg = ','.join(row)
                channel.basic_publish(exchange='', routing_key='data', body=msg)
                time.sleep(self.config["SENDING_WAIT_TIME"])
        

        channel.basic_publish(exchange='', routing_key=self.config["OUTPUT_QUEUE"], body=FILE_END_MSG)


    def sigterm_handler(self, signal, frame):
        self.got_sigterm = True