import logging
import signal
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

        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def start_results_middleware(self):
        self.results_middleware = Middleware(
            "rabbitmq",
            protocol=Protocol(),
        )
        self.results_middleware.create_queue(name="Q1")
        self.results_middleware.create_queue(name="Q2")
        self.results_middleware.create_queue(name="Q3")
        self.results_middleware.create_queue(name="Q4")
        self.results_middleware.create_queue(name="Q5")
        self.results_middleware.attach_callback("Q1", self.on_message)
        self.results_middleware.attach_callback("Q2", self.on_message)
        self.results_middleware.attach_callback("Q3", self.on_message)
        self.results_middleware.attach_callback("Q4", self.on_message)
        self.results_middleware.attach_callback("Q5", self.on_message)
        self.results_middleware.start_consuming()

    def a(self):
        self._client_middleware.create_socket(zmq.ROUTER)
        self._client_middleware.bind(self._port)
        self._client_middleware.register_for_pollin()
        while not self._got_sigterm:
            if self._client_middleware.has_message():
                client_id, message = self._client_middleware.recv_multipart()
                client_id_hex = client_id.hex()
                logging.debug(f"Received message from {client_id_hex}: {message}")

                if client_id not in self._forwarding_queues_per_client.keys():
                    logging.debug("Setting forwarding queue to games")
                    self._forwarding_queues_per_client[client_id] = (
                        self._games_queue_name
                    )

                forwarding_queue_name = self._forwarding_queues_per_client[client_id]

                self._middleware.add_client_id_and_send_batch(
                    queue_name=forwarding_queue_name,
                    client_id=client_id_hex,
                    batch=message,
                )

                if message[-3:] == b"END":
                    logging.debug("Setting forwarding queue to reviews")
                    self._forwarding_queues_per_client[client_id] = (
                        self._reviews_queue_name
                    )

        self._client_middleware.shutdown()
        self._middleware.shutdown()

    def run(self):
        thread = threading.Thread(target=self.a)
        thread.start()
        try:
            self.start_results_middleware()
        except SystemExit:
            logging.info("Shutting down results middleware...")
        finally:
            self.results_middleware.shutdown()

    def on_message(self, channel, method_frame, header_frame, body):
        logging.debug(f"received results from queue: {method_frame.routing_key}")

        client_id = bytes.fromhex(
            self.results_middleware.get_rows_from_message(body)[0][0]
        )
        self._client_middleware.send_query_results(
            client_id, message=body, query=method_frame.routing_key
        )
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def __sigterm_handler(self, sig, frame):
        logging.info("Shutting down Client Handler")
        self._got_sigterm = True
        raise SystemExit
