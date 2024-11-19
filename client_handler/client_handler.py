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

CLIENT_INFO_QUEUE_NAME_INDEX = 0
CLIENT_INFO_TIMER_INDEX = 1
CLIENT_INFO_CONNECTION_ID_INDEX = 2
CLIENT_INFO_FINISHED_QUERIES_INDEX = 3
CLIENT_INFO_FINISHED_QUERIES_LOCK_INDEX = 4
CLIENT_INFO_LAST_MESSAGE_INDEX = 5


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

    def process_message(self, msg_type: str, connection_id, message: bytes):
        # N: New session
        # R: Return session
        # H: Heartbeat
        # C: Continue?

        logging.debug(f"Received message of type: {msg_type} | message: {message}")

        if msg_type == "D":
            session_id, message = self._client_middleware.get_session_id(message)
            # logging.info(f"Last message number: {message_number}")
            client_info = self._clients_info[session_id]

            # Cancel timer
            client_info[CLIENT_INFO_TIMER_INDEX].cancel()

            forwarding_queue_name = client_info[CLIENT_INFO_QUEUE_NAME_INDEX]

            self._middleware.add_client_id_and_send_batch(
                queue_name=forwarding_queue_name,
                client_id=session_id,
                batch=message,
            )

            client_info[CLIENT_INFO_LAST_MESSAGE_INDEX] = (
                self._client_middleware.get_last_message_id(message)
            )

            if message[-3:] == b"END":
                logging.debug(f"Received  END from session: {session_id}")

                if forwarding_queue_name == self._reviews_queue_name:
                    logging.info(f"Final end received from client: {session_id}.")
                    return

                logging.debug("Setting forwarding queue to reviews")

                client_info[CLIENT_INFO_QUEUE_NAME_INDEX] = self._reviews_queue_name

            t = threading.Timer(100000.0, self.handle_client_timeout, args=[session_id])
            client_info[CLIENT_INFO_TIMER_INDEX] = t
            t.start()

        elif msg_type == "Q":
            rows = self._middleware.get_rows_from_message(message)
            logging.info(f"Req: {rows}")

            self._send_query_results_if_there_are(
                session_id=rows[0][0], query=rows[1][0]
            )
        elif msg_type == "H":
            rows = self._client_middleware.get_row_from_message(message)
            session_id = rows[0]

            logging.debug(f"Received heartbeat from {session_id}")
            self._client_middleware.send_multipart(
                connection_id=self._clients_info[session_id][
                    CLIENT_INFO_CONNECTION_ID_INDEX
                ],
                message=[
                    "H",
                    rows[1],
                ],  # rows[1] contains heartbeat id, its used for detecting duplicates
                needs_encoding=True,
            )
        elif msg_type == "C":
            rows = self._client_middleware.get_row_from_message(message)
            session_id = rows[0]
            logging.info(f"Received last message from {session_id} | {rows}")
            client_info = self._clients_info[session_id]

            self._client_middleware.send_multipart(
                connection_id=client_info[CLIENT_INFO_CONNECTION_ID_INDEX],
                message=[
                    "C",
                    rows[
                        1
                    ],  # contains restart session id, its used for detecting duplicates
                    str(client_info[CLIENT_INFO_LAST_MESSAGE_INDEX]),
                ],
                needs_encoding=True,
            )

        elif msg_type == "N":
            logging.debug("Setting forwarding queue to games")
            session_id = str(uuid.uuid4())

            t = threading.Timer(100000.0, self.handle_client_timeout, args=[session_id])

            self._clients_info[session_id] = [
                self._games_queue_name,
                t,
                connection_id,
                set(),
                threading.Lock(),
                0,
            ]

            self._client_middleware.send_multipart(
                connection_id, [session_id], needs_encoding=True
            )

            t.start()
        else:
            logging.info(f"Message type: {msg_type} not recognized")

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

                # message format: (msg_type, msg)
                # -> msg must contain (if its the case) the session id
                connection_id, msg_type, msg = self._client_middleware.recv_multipart()
                # logging.info(f"Session: {connection_id} | msg_type: {msg_type}")
                self.process_message(msg_type, connection_id, msg)

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

        # remove all client data
        client_info = self._clients_info[client_id]
        storage.delete_files_from_directory(f"/tmp/{client_id}")

        client_info[CLIENT_INFO_TIMER_INDEX].cancel()
        # TODO: Ver que onda si se cae por aca
        del client_info

    def handle_client_timeout(self, client_id):
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
        rows = self.results_middleware.get_rows_from_message(body)
        logging.info(f"Rows: {rows}")

        client_id = rows[0][0]
        query = method_frame.routing_key

        logging.info(f"received results from queue: {query} from client: {client_id}")

        connection_id = self._clients_info[client_id][CLIENT_INFO_CONNECTION_ID_INDEX]
        batch_per_client, client_ends = self._get_batch_per_client_and_ends(rows)

        # Update client info
        storage.save_multiclient_batch(
            dir="/tmp",
            batchs_per_client=batch_per_client,
            file_name=query,
        )

        self._add_finished_query_to_clients(clients=client_ends, query=query)

        # self._client_middleware.send_query_results(
        #     connection_id, message=body, query=query
        # )

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def __sigterm_handler(self, sig, frame):
        logging.info("Shutting down Client Handler")
        self._got_sigterm.set()
        raise SystemExit

    # Given a batch that can possibly contain multiple clients info (only used for query results),
    # returns a dictionary with client_id: batch, and also a set with client_ids, which represents
    # all the ENDs found in the batch (for every query), in order to identify them
    def _get_batch_per_client_and_ends(
        self, records: list[list[str]]
    ) -> Tuple[dict[str, list[str]], set[str]]:
        batch_per_client = {}
        clients_ends = set()
        # Get the batch for every client
        for record in records:
            client_id = record[0]
            record = record[1:]
            if record[0] == "END":
                clients_ends.add(client_id)
            if not client_id in batch_per_client:
                batch_per_client[client_id] = []

            batch_per_client[client_id].append(record)
        return (batch_per_client, clients_ends)

    def _add_finished_query_to_clients(self, clients: set[str], query: str):
        for client_id in clients:
            client_info = self._clients_info[client_id]

            with client_info[CLIENT_INFO_FINISHED_QUERIES_LOCK_INDEX]:
                client_info[CLIENT_INFO_FINISHED_QUERIES_INDEX].add(query)
                logging.info(
                    f"Client: {client_id} | {self._clients_info[client_id][CLIENT_INFO_FINISHED_QUERIES_INDEX]}"
                )

    def _send_query_results_if_there_are(self, session_id: str, query: str):
        try:
            client_info = self._clients_info[session_id]
            with client_info[CLIENT_INFO_FINISHED_QUERIES_LOCK_INDEX]:
                if query in client_info[CLIENT_INFO_FINISHED_QUERIES_INDEX]:
                    self._read_and_send_results(session_id, query)

        except KeyError:
            logging.error(
                f"Not processing results request from {session_id}. No client was found with that session."
            )

    def _read_and_send_results(self, session_id: str, query: str):
        client_info = self._clients_info[session_id]
        res = storage.read(f"/tmp/{session_id}/{query}")

        self._client_middleware.send_query_results_from_generator(
            client_id=client_info[CLIENT_INFO_CONNECTION_ID_INDEX],
            results=res,
            query=query,
        )

        logging.info(
            f"Sent results of query: {query} to client with sesion: {session_id}"
        )
