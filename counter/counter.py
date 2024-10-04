import signal
import logging
from common.middleware import *
from common.storage.storage import *

END_TRANSMISSION_MESSAGE = 'END' 

class Counter:

    def __init__(self, config, middleware:Middleware):
        self.config = config
        self.middleware = middleware
        self.got_sigterm = False
        self.count_dict = {}
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):
        
        consume_queue_name = f"{self.config['NODE_ID']}_{self.config['CONSUME_QUEUE_SUFIX']}"
        self.middleware.create_queue(consume_queue_name)
        self.middleware.create_queue(self.config["PUBLISH_QUEUE"])

        self.middleware.attach_callback(consume_queue_name, self.handle_message)
        
        try:
            logging.debug("Starting to consume...")
            self.middleware.start_consuming()
        except OSError as _:
            if not self.got_sigterm: raise

    
    def handle_message(self, ch, method, properties, body):

        msg = body.decode('utf-8').strip()

        logging.info(f"GOT MSG: {msg}")

        if msg == "END":
            self.send_results()
            self.middleware.ack(method.delivery_tag)
            return

        field_to_count = msg.split(",")[0]
        self.count(field_to_count)

        self.middleware.ack(method.delivery_tag)

    
    def send_results(self):
        # TODO: READ FROM STORAGE
        
        for key, value in self.count_dict.items():
            if self.got_sigterm:
                return

            msg = ",".join([key,str(value)])
            self.middleware.publish(msg, queue_name=self.config["PUBLISH_QUEUE"])

        self.middleware.publish(END_TRANSMISSION_MESSAGE, queue_name=self.config["PUBLISH_QUEUE"])
    
    def count(self, field_to_count):
        # TODO: SAVE AND UPDATE ON STORAGE
        
        actual_count = self.count_dict.get(field_to_count, 0)
        self.count_dict[field_to_count] = actual_count + 1
        

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self.got_sigterm = True
        self.middleware.shutdown()