import logging
import threading
from typing import *
import uuid
import zmq
import pika

from common.client_middleware.client_middleware import ClientMiddleware
from common.middleware.middleware import Middleware, MiddlewareError
from common.protocol.protocol import Protocol
from common.storage import storage

END_TRANSMISSION_MESSAGE = "END"
APP_ID = 0


class ClientHandler:

    def __init__(
        self, middleware: Middleware, client_middleware: ClientMiddleware, **kwargs
    ):
        self._middleware = middleware
        self._client_middleware = client_middleware
        self._got_sigterm = False
        self._ends_received = 0
        self._port = kwargs["CLIENTS_PORT"]
        self._games_queue_name = kwargs["GAMES_QUEUE_NAME"]
        self._reviews_queue_name = kwargs["REVIEWS_QUEUE_NAME"]
        self._forwarding_queues_per_client = {}
        # signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def a(self):
        parameters = pika.ConnectionParameters(host="rabbitmq")
        connection = pika.SelectConnection(
            parameters, on_open_callback=self.on_connected
        )
        connection.ioloop.start()

    def run(self):
        thread = threading.Thread(target=self.a)
        thread.start()
        self._client_middleware.create_socket(zmq.ROUTER)
        self._client_middleware.bind(self._port)
        while True:
            client_id, message = self._client_middleware.recv_multipart()
            client_id_hex = client_id.hex()
            logging.debug(f"Received message from {client_id_hex}: {message}")

            if client_id not in self._forwarding_queues_per_client.keys():
                logging.debug("Setting forwarding queue to games")
                self._forwarding_queues_per_client[client_id] = self._games_queue_name

            forwarding_queue_name = self._forwarding_queues_per_client[client_id]

            self._middleware.add_client_id_and_send_batch(
                queue_name=forwarding_queue_name,
                client_id=client_id_hex,
                batch=message,
            )

            if message[-3:] == b"END":
                logging.debug("Setting forwarding queue to reviews")
                self._forwarding_queues_per_client[client_id] = self._reviews_queue_name

    def on_message(self, channel, method_frame, header_frame, body):
        protocol = Protocol()
        body = protocol.decode_batch(body)
        logging.debug(f"Results: {body}")
        client_id = bytes.fromhex(body[0][0])
        actual = b""

        for message in body:
            _ = message.pop(0)
            actual = protocol.add_to_batch(actual, message)

        self._client_middleware.send_multipart(client_id, actual)
        logging.debug(f"Sent: {actual}")

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def on_connected(self, connection):
        connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        channel.queue_declare(queue="Q1")
        channel.queue_declare(queue="Q2")
        channel.queue_declare(queue="Q3")
        channel.queue_declare(queue="Q4")
        channel.queue_declare(queue="Q5")
        channel.basic_consume("Q1", on_message_callback=self.on_message)
        channel.basic_consume("Q2", on_message_callback=self.on_message)
        channel.basic_consume("Q3", on_message_callback=self.on_message)
        channel.basic_consume("Q4", on_message_callback=self.on_message)
        channel.basic_consume("Q5", on_message_callback=self.on_message)
        logging.debug("Consuming stuff")
