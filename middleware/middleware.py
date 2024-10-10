import logging
import pika

from common.protocol.protocol import Protocol

END_TRANSMISSION_MESSAGE = "END"

class MiddlewareError(Exception):
    def __init__(self, message=None):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return (
            f"MiddlewareError: {self.message}" 
            if self.message else 
            "MiddlewareError has occurred"
        )
    
class Middleware:
    def __init__(self, broker_ip, protocol: Protocol = Protocol()):
        self._connection = self.__create_connection(broker_ip)
        self._channel = self._connection.channel()
        self.__protocol = protocol
        self.__batch_size = 10  # TODO: receive as param
        self.__max_batch_size = 1 * 1024  # TODO: receive as param
        self.__batchs_per_queue = {}

    def __create_connection(self, ip):
        return pika.BlockingConnection(pika.ConnectionParameters(host=ip))

    def create_queue(self, name):
        self.__batchs_per_queue[name] = [
            b"",
            0,
        ]  # [messages encoded, amount of messages]
        self._channel.queue_declare(queue=name)

    def attach_callback(self, queue_name, callback):
        self._channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=False
        )

    def turn_fair_dispatch(self):
        # Fairness
        self._channel.basic_qos(prefetch_count=1)

    def publish_batch(self, queue_name="", exchange_name=""):

        batch, amount_of_messages = self.__batchs_per_queue[queue_name]

        if amount_of_messages == 0:
            return

        self._channel.basic_publish(
            exchange=exchange_name, routing_key=queue_name, body=batch
        )

        self.__batchs_per_queue[queue_name] = [b"", 0]

    def publish_message(self, message: list[str], queue_name="", exchange_name=""):
        self._channel.basic_publish(
            exchange=exchange_name,
            routing_key=queue_name,
            body=self.__protocol.add_to_batch(current_batch=b"", row=message),
        )

    def publish(self, message: list[str], queue_name="", exchange_name=""):
        queue_batch, amount_of_messages = self.__batchs_per_queue[queue_name]
        new_batch = self.__protocol.add_to_batch(queue_batch, message)

        # if len(new_batch) > self.__max_batch_size:
        if amount_of_messages + 1 >= self.__batch_size:
            self._channel.basic_publish(
                exchange=exchange_name,
                routing_key=queue_name,
                body=queue_batch,
            )
            self.__batchs_per_queue[queue_name] = [
                self.__protocol.add_to_batch(b"", message),
                1,
            ]
        else:
            self.__batchs_per_queue[queue_name] = [new_batch, amount_of_messages + 1]

    def get_rows_from_message(self, message) -> list[list[str]]:
        return self.__protocol.decode_batch(message)

    def send_end(self, queue, exchange_name="", end_message=END_TRANSMISSION_MESSAGE):
        end_message = self.__protocol.add_to_batch(current_batch=b"", row=[end_message])
        self._channel.basic_publish(
            exchange=exchange_name, routing_key=queue, body=end_message
        )

    def start_consuming(self):
        try:
            self._channel.start_consuming()
        except pika.exceptions.ChannelClosedByBroker as _: 
            # Rabbit mq terminated during execution most probably
            # TODO: Is writing to a closed channel handled by this too or
            # does pika.exceptions.ClosedChannel need to be accounted for? 
            raise MiddlewareError(message="Channel was closed by borker")
        except pika.exceptions.ConnectionClosed as _:
            # Connection was finished either due to shutdown
            # or general network error
            raise MiddlewareError(message="A connection error ocurred with the broker")
        except OSError as _:
            raise MiddlewareError("Attempted to send data to a closed socket")

    def stop_consuming(self):
        self._channel.stop_consuming()

    def ack(self, delivery_tag):
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def shutdown(self):
        self._channel.stop_consuming()
        self._connection.close()

    # Callback should be a function that recives:
    # - delivery_tag: so that it can ack the corresponding message
    # - body: the content of the message itself as bytes
    # - *args: any extra arguments necesary
    @classmethod
    def generate_callback(cls, callback, *args):
        return lambda ch, method, props, body: callback(
            method.delivery_tag, body, *args
        )
