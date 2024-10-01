import pika

class Middleware:
    
    def __init__(self, broker_ip):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(broker_ip))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)

    def declare_queue(self, queue):
        self.channel.queue_declare(queue=queue, durable=True)

    def send(self, queue_name, msg):
        self.channel.basic_publish(exchange='', routing_key=queue_name, body=msg)

    def consume_from_queue(self, queue, callback):
        self.channel.basic_consume(queue=queue, auto_ack=False, on_message_callback=callback)
        self.channel.start_consuming()

    def ack(self, tag):
        self.channel.basic_ack(delivery_tag = tag)

    def stop_consuming(self):
        self.channel.stop_consuming()

    def close(self):
        self.channel.close()
        self.connection.close()
