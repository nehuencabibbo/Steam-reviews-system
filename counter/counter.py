import signal
import logging
from common.middleware import *
from common.storage.storage import *
from common.protocol.protocol import Protocol

END_TRANSMISSION_MESSAGE = 'END' 

class Counter:

    def __init__(self, config, middleware:Middleware, protocol: Protocol):
        self.protocol = protocol
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

        body = self.protocol.decode(body)
        body = [value.strip() for value in body]

        logging.info(f"GOT MSG: {body}")

        if len(body) == 1 and body[0] == "END":
            self.send_results()
            self.middleware.ack(method.delivery_tag)
            return

        field_to_count = body[0]
        self.count(field_to_count)

        self.middleware.ack(method.delivery_tag)

    
    def send_results(self):
        # TODO: READ FROM STORAGE
        
        for key, value in self.count_dict.items():
            if self.got_sigterm:
                return

            encoded_msg = self.protocol.encode([key,str(value)])
            self.middleware.publish(encoded_msg, queue_name=self.config["PUBLISH_QUEUE"])

        encoded_msg = self.protocol.encode([END_TRANSMISSION_MESSAGE])
        self.middleware.publish(encoded_msg, queue_name=self.config["PUBLISH_QUEUE"])
    
    def count(self, field_to_count):
        # TODO: SAVE AND UPDATE ON STORAGE
        
        actual_count = self.count_dict.get(field_to_count, 0)
        self.count_dict[field_to_count] = actual_count + 1
        

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self.got_sigterm = True
        self.middleware.shutdown()