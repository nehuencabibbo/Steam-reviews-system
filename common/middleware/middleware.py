import logging
import pika

from common.protocol.protocol import Protocol

END_TRANSMISSION_MESSAGE = "END"


class Middleware:
    def __init__(self, broker_ip, protocol: Protocol = Protocol()):
        self._connection = self.__create_connection(broker_ip)
        self._channel = self._connection.channel()
        self.__protocol = protocol
        self.__batch_size = 10  # TODO: receive as param
        self.__max_batch_size = 4 * 1024  # TODO: receive as param
        self.__batchs_per_queue = {}

    def __create_connection(self, ip):
        return pika.BlockingConnection(pika.ConnectionParameters(host=ip))

    def create_queue(self, name):
        self.__batchs_per_queue[name] = b""
        self._channel.queue_declare(queue=name)

    def attach_callback(self, queue_name, callback):
        self._channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=False
        )

    def turn_fair_dispatch(self):
        # Fairness
        self._channel.basic_qos(prefetch_count=1)

    def publish(self, message: list[str], queue_name="", exchange_name="", batch=True):
        # Otherwise the END, peer_1,.. are not sent
        if not batch:
            self._channel.basic_publish(
                exchange=exchange_name,
                routing_key=queue_name,
                body=self.__protocol.add_to_batch(current_batch=b"", row=message),
            )
            return

        new_batch = self.__protocol.add_to_batch(
            self.__batchs_per_queue[queue_name], message
        )

        if len(new_batch) > self.__max_batch_size:
            logging.debug(
                f"Length exceeded, evicting. Before: {len(self.__batchs_per_queue[queue_name])} | After: {len(new_batch)}"
            )
            self._channel.basic_publish(
                exchange=exchange_name,
                routing_key=queue_name,
                body=self.__batchs_per_queue[queue_name],
            )
            self.__batchs_per_queue[queue_name] = self.__protocol.add_to_batch(
                b"", message
            )
        else:
            self.__batchs_per_queue[queue_name] = new_batch

    def get_rows_from_message(self, message) -> list[list[str]]:
        return self.__protocol.decode_batch(message)

    def send_end(self, queue, exchange_name="", end_message=END_TRANSMISSION_MESSAGE):
        self._channel.basic_publish(
            exchange=exchange_name,
            routing_key=queue,
            body=self.__batchs_per_queue[queue],
        )

        self.__batchs_per_queue[queue] = b""

        end_message = self.__protocol.add_to_batch(current_batch=b"", row=[end_message])
        self._channel.basic_publish(
            exchange=exchange_name, routing_key=queue, body=end_message
        )

    def start_consuming(self):
        self._channel.start_consuming()

    def stop_consuming(self):
        self._channel.stop_consuming()

    def ack(self, delivery_tag):
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def shutdown(self):
        self._channel.stop_consuming()
        self.connection.close()

    # Callback should be a function that recives:
    # - delivery_tag: so that it can ack the corresponding message
    # - body: the content of the message itself as bytes
    # - *args: any extra arguments necesary
    @classmethod
    def generate_callback(cls, callback, *args):
        return lambda ch, method, props, body: callback(
            method.delivery_tag, body, *args
        )
