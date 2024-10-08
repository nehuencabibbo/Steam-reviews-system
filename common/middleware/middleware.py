import pika


class Middleware:

    def __init__(self, broker_ip):
        self.connection = self.__create_connection(broker_ip)
        self._channel = self.connection.channel()

    def __create_connection(self, ip):
        return pika.BlockingConnection(pika.ConnectionParameters(host=ip))

    def create_queue(self, queue):
        self._channel.queue_declare(queue=queue)

    def attach_callback(self, queue_name, callback):
        self._channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=False
        )

    def publish(self, message, queue_name="", exchange_name=""):
        self._channel.basic_publish(
            exchange=exchange_name, routing_key=queue_name, body=message
        )

    def start_consuming(self):
        self._channel.start_consuming()

    def ack(self, tag):
        self._channel.basic_ack(delivery_tag=tag)

    def stop_consuming(self):
        self._channel.stop_consuming()

    def shutdown(self):
        self.stop_consuming()
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
