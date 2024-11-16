import functools
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
        self._ends_received = 0
        self._port = kwargs["CLIENTS_PORT"]
        self._games_queue_name = kwargs["GAMES_QUEUE_NAME"]
        self._reviews_queue_name = kwargs["REVIEWS_QUEUE_NAME"]
        self._clients_info = {}
        self._rabbit_ip = kwargs["RABBIT_IP"]
        self._q1_result_queue = kwargs["Q1_RESULT_QUEUE"]
        self._q2_result_queue = kwargs["Q2_RESULT_QUEUE"]
        self._q3_result_queue = kwargs["Q3_RESULT_QUEUE"]
        self._q4_result_queue = kwargs["Q4_RESULT_QUEUE"]
        self._q5_result_queue = kwargs["Q5_RESULT_QUEUE"]
        self._got_sigterm = threading.Event()
        logging.info(f"_reviews_queue_name: {self._reviews_queue_name}")

        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def start_results_middleware(self):
        self.results_middleware = Middleware(
            self._rabbit_ip,
            protocol=Protocol(),
        )
        self.results_middleware.create_queue(name=self._q1_result_queue)
        self.results_middleware.create_queue(name=self._q2_result_queue)
        self.results_middleware.create_queue(name=self._q3_result_queue)
        self.results_middleware.create_queue(name=self._q4_result_queue)
        self.results_middleware.create_queue(name=self._q5_result_queue)
        self.results_middleware.attach_callback(self._q1_result_queue, self.on_message)
        self.results_middleware.attach_callback(self._q2_result_queue, self.on_message)
        self.results_middleware.attach_callback(self._q3_result_queue, self.on_message)
        self.results_middleware.attach_callback(self._q4_result_queue, self.on_message)
        self.results_middleware.attach_callback(self._q5_result_queue, self.on_message)
        self.results_middleware.start_consuming()

    def process_message(self, msg_type, connection_id, message):
        # N: New session
        # R: Return session
        # H: Heartbeat

        logging.debug(f"Received message of type: {msg_type} | message: {message}")

        if msg_type == "N":
            logging.debug("Setting forwarding queue to games")
            session_id = str(uuid.uuid4())

            t = (
                threading.Timer(
                    100000.0, self.handle_client_timeout, args=[session_id]
                ),
            )

            self._clients_info[session_id] = [
                self._games_queue_name,
                t[0],
                connection_id,
            ]

            self._client_middleware.send_multipart(
                connection_id, session_id, needs_encoding=True
            )

            t[0].start()
        else:
            session_id, message = self._client_middleware.get_session_id(message)
            logging.info(f"SESSION_ID: {session_id}")

            forwarding_queue_name = self._clients_info[session_id][0]

            self._middleware.add_client_id_and_send_batch(
                queue_name=forwarding_queue_name,
                client_id=session_id,
                batch=message,
            )

            if message[-3:] == b"END":
                logging.debug(f"Received  END from session: {session_id}")
                # TODO: publish batch
                if forwarding_queue_name == self._reviews_queue_name:
                    logging.info(f"Final end received from client: {session_id}.")
                    # self._clients_info[session_id][1].cancel()
                    # del self._clients_info[session_id]
                    return
                logging.debug("Setting forwarding queue to reviews")
                # TODO: update timer

                client_info = self._clients_info[session_id]
                client_info[1].cancel()

                t = (
                    threading.Timer(
                        100000.0, self.handle_client_timeout, args=[session_id]
                    ),
                )

                self._clients_info[session_id] = (
                    self._reviews_queue_name,
                    t[0],
                    client_info[2],
                )
                t[0].start()

    def handle_clients(self):
        self._client_middleware.create_socket(zmq.ROUTER)
        try:
            self._client_middleware.bind(self._port)
            self._client_middleware.register_for_pollin()
            while not self._got_sigterm.is_set():
                logging.debug("Pollin")
                # for answering heartbeats if there is any
                self._middleware.process_events_once()

                self._middleware._connection.process_data_events(time_limit=0)
                if not self._client_middleware.has_message():
                    continue

                connection_id, msg_type, msg = self._client_middleware.recv_multipart()
                logging.info(f"Session: {connection_id} | msg_type: {msg_type}")
                self.process_message(msg_type, connection_id, msg)
                # client_id_hex = client_id.hex()
                # logging.debug(f"Received message from {client_id_hex}: {message}")

                # if client_id not in self._clients_info.keys():
                #     logging.debug("Setting forwarding queue to games")
                #     t = (
                #         threading.Timer(
                #             100000.0, self.handle_client_timeout, args=[client_id_hex]
                #         ),
                #     )

                #     self._clients_info[client_id] = [
                #         self._games_queue_name,
                #         t[0],
                #     ]

                #     t[0].start()

                # forwarding_queue_name = self._clients_info[client_id][0]

                # self._middleware.add_client_id_and_send_batch(
                #     queue_name=forwarding_queue_name,
                #     client_id=client_id_hex,
                #     batch=message,
                # )

                # if message[-3:] == b"END":
                #     # TODO: publish batch
                #     if forwarding_queue_name == self._reviews_queue_name:
                #         logging.info(
                #             f"Final end received from client: {client_id}. Removing from the record."
                #         )
                #         del self._clients_info[client_id]
                #         continue

                #     logging.debug("Setting forwarding queue to reviews")
                #     # TODO: update timer
                #     self._clients_info[client_id] = (
                #         self._reviews_queue_name
                #     )
        except MiddlewareError as e:
            logging.error(e.message)
        finally:
            self._client_middleware.shutdown()
            self._middleware.shutdown()

    def send_timeout(self, client_id):
        logging.info(f"Executing send_timeout for client: {client_id}")
        self._middleware.publish_message(
            [client_id, "TIMEOUT"], self._reviews_queue_name
        )
        self._middleware.publish_message([client_id, "TIMEOUT"], self._games_queue_name)

    def handle_client_timeout(self, client_id):
        # del
        self._middleware.execute_from_another_thread(
            functools.partial(self.send_timeout, client_id=client_id)
        )

    def run(self):
        thread = threading.Thread(target=self.handle_clients)
        thread.start()

        try:
            self.start_results_middleware()
        except MiddlewareError as e:
            logging.error(e.message)

        except SystemExit:
            logging.info("Shutting down results middleware...")
        finally:
            self.results_middleware.shutdown()

    def on_message(self, channel, method_frame, header_frame, body):
        client_id = self.results_middleware.get_rows_from_message(body)[0][0]

        logging.info(
            f"received results from queue: {method_frame.routing_key} from client: {client_id}"
        )

        connection_id = self._clients_info[client_id][2]

        self._client_middleware.send_query_results(
            connection_id, message=body, query=method_frame.routing_key
        )
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def __sigterm_handler(self, sig, frame):
        logging.info("Shutting down Client Handler")
        self._got_sigterm.set()
        raise SystemExit
