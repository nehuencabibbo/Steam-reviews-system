import ast
import functools
import logging
import random
import signal
import threading
import time
from typing import *
import uuid
from common.activity_log.activity_log import ActivityLog
from common.storage import storage
import zmq
import pika
from constants import *

from common.client_middleware.client_middleware import ClientMiddleware
from common.middleware.middleware import Middleware, MiddlewareError
from common.protocol.protocol import Protocol
from common.watchdog_client.watchdog_client import WatchdogClient


class ClientHandler:

    def __init__(
        self,
        middleware: Middleware,
        client_middleware: ClientMiddleware,
        client_monitor: WatchdogClient,
        logger: ActivityLog,
        **kwargs,
    ):
        self._middleware = middleware
        self._client_middleware = client_middleware
        self._port = kwargs["CLIENTS_PORT"]
        self._games_queue_name = kwargs["GAMES_QUEUE_NAME"]
        self._reviews_queue_name = kwargs["REVIEWS_QUEUE_NAME"]
        self._activity_log = logger
        # self._clients_info = {}

        self._rabbit_ip = kwargs["RABBIT_IP"]
        self._q1_result_queue = kwargs["Q1_RESULT_QUEUE"]
        self._q2_result_queue = kwargs["Q2_RESULT_QUEUE"]
        self._q3_result_queue = kwargs["Q3_RESULT_QUEUE"]
        self._q4_result_queue = kwargs["Q4_RESULT_QUEUE"]
        self._q5_result_queue = kwargs["Q5_RESULT_QUEUE"]
        self._got_sigterm = threading.Event()
        self._client_monitor = client_monitor
        self._clients_info = self._activity_log.recover_client_handler_state()
        self._activity_log_lock = threading.Lock()

        for k, v in self._clients_info.items():
            logging.info(f"Recovered: {k} | {v}")
            t = threading.Timer(30.0, self.handle_client_timeout, args=[k])
            # Conversion from str to bytes
            v[CLIENT_INFO_CONNECTION_ID_INDEX] = ast.literal_eval(
                v[CLIENT_INFO_CONNECTION_ID_INDEX]
            )

            v.extend(
                [
                    t,
                    threading.Lock(),
                ]
            )

            t.start()

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

    def process_data_message(self, message: bytes):
        if random.randint(0, 9) > 2:
            logging.info(f"Bad luck, message dropped")
            return

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

        last_message_id_from_batch = self._client_middleware.get_last_message_id(
            message
        )

        # this if is made in order not to check against the normal batch size
        if (
            message[-3:] == b"END"
            and last_message_id_from_batch
            != client_info[CLIENT_INFO_LAST_MESSAGE_INDEX] + 1
        ):
            logging.error(
                f"Missing messages. Expected: {client_info[CLIENT_INFO_LAST_MESSAGE_INDEX] + 1}, got: {last_message_id_from_batch}. Not updating last acked message for session: {session_id}"
            )
            return

        if (
            client_info[CLIENT_INFO_LAST_MESSAGE_INDEX] != 0
            and last_message_id_from_batch - client_info[CLIENT_INFO_LAST_MESSAGE_INDEX]
            > client_info[CLIENT_INFO_BATCH_SIZE]
        ):
            logging.error(
                f"Missing messages. Expected: {client_info[CLIENT_INFO_BATCH_SIZE] + last_message_id_from_batch} or less, got: {last_message_id_from_batch}. Not updating last acked message for session: {session_id}. client_info[CLIENT_INFO_LAST_MESSAGE_INDEX]: {client_info[CLIENT_INFO_LAST_MESSAGE_INDEX]}"
            )
            return

        # client_info[CLIENT_INFO_NEXT_BATCH_EXPECTED_INDEX] = (
        #     last_message_id_from_batch + batch_size
        #     if message[-3:] != b"END"
        #     else last_message_id_from_batch + 1
        # )

        client_info[CLIENT_INFO_LAST_MESSAGE_INDEX] = last_message_id_from_batch

        if message[-3:] == b"END":
            logging.debug(f"Received  END from session: {session_id}")

            if forwarding_queue_name == self._reviews_queue_name:
                logging.info(f"Final end received from client: {session_id}.")
                with self._activity_log_lock:
                    self._activity_log.log_for_client_handler(
                        client_id=session_id,
                        queue_name=client_info[CLIENT_INFO_QUEUE_NAME_INDEX],
                        connection_id=client_info[CLIENT_INFO_CONNECTION_ID_INDEX],
                        last_ack_message=client_info[CLIENT_INFO_LAST_MESSAGE_INDEX],
                        finished_querys=client_info[CLIENT_INFO_FINISHED_QUERIES_INDEX],
                    )

                t = threading.Timer(30.0, self.handle_client_timeout, args=[session_id])
                client_info[CLIENT_INFO_TIMER_INDEX] = t

                t.start()

                return

            logging.debug("Setting forwarding queue to reviews")

            client_info[CLIENT_INFO_QUEUE_NAME_INDEX] = self._reviews_queue_name
        with self._activity_log_lock:
            self._activity_log.log_for_client_handler(
                client_id=session_id,
                queue_name=client_info[CLIENT_INFO_QUEUE_NAME_INDEX],
                connection_id=client_info[CLIENT_INFO_CONNECTION_ID_INDEX],
                last_ack_message=client_info[CLIENT_INFO_LAST_MESSAGE_INDEX],
                finished_querys=client_info[CLIENT_INFO_FINISHED_QUERIES_INDEX],
            )

        t = threading.Timer(30.0, self.handle_client_timeout, args=[session_id])
        client_info[CLIENT_INFO_TIMER_INDEX] = t

        t.start()

    def process_query_results_request_message(self, message: bytes):
        rows = self._middleware.get_rows_from_message(message)
        session_id = rows[0][0]

        self._clients_info[session_id][CLIENT_INFO_TIMER_INDEX].cancel()

        logging.debug(f"Req: {rows}")

        t = threading.Timer(30.0, self.handle_client_timeout, args=[session_id])
        self._clients_info[session_id][CLIENT_INFO_TIMER_INDEX] = t

        self._send_query_results_if_there_are(session_id, query=rows[1][0])
        t.start()

    # def process_session_restart_message(self, message: bytes):
    #     # if random.randint(0, 9) > 8:
    #     #     logging.info(f"Bad luck, message of type: {msg_type} dropped")
    #     #     return
    #     rows = self._client_middleware.get_row_from_message(message)
    #     session_id = rows[0]
    #     self._clients_info[session_id][CLIENT_INFO_TIMER_INDEX].cancel()

    #     # logging.debug(f"Req: {rows}")

    #     t = threading.Timer(30.0, self.handle_client_timeout, args=[session_id])
    #     self._clients_info[session_id][CLIENT_INFO_TIMER_INDEX] = t

    #     logging.info(f"Received session restart from: {session_id} | {rows}")
    #     client_info = self._clients_info[session_id]

    #     self._client_middleware.send_multipart(
    #         connection_id=client_info[CLIENT_INFO_CONNECTION_ID_INDEX],
    #         message=[
    #             "C",
    #             rows[
    #                 1
    #             ],  # contains restart session id, its used for detecting duplicates
    #             str(client_info[CLIENT_INFO_LAST_MESSAGE_INDEX]),
    #         ],
    #         needs_encoding=True,
    #     )
    #     t.start()

    def process_new_session_request_message(self, connection_id, message: bytes):
        # if random.randint(0, 9) < 6:
        #     logging.error(f"Bad luck, message of type: {msg_type} dropped")
        #     return

        session_id = str(uuid.uuid4())
        logging.info(f"New session request, assigned session id: {session_id}")
        self._client_middleware.send_multipart(
            connection_id, ["N", session_id], needs_encoding=True
        )
        # logging.debug("Setting forwarding queue to games")

    def process_session_ack_message(self, connection_id, message: bytes):
        # if random.randint(0, 9) < 3:
        #     logging.error(f"Bad luck, message of type: {msg_type} dropped")
        #     return

        rows = self._client_middleware.get_row_from_message(message)
        session_id = rows[0]
        logging.info(f"Session: {session_id} successfully connected | rows: {rows}")
        t = threading.Timer(30.0, self.handle_client_timeout, args=[session_id])

        self._clients_info[session_id] = [
            self._games_queue_name,  # No es fijo, pero cambia una vez nomas
            connection_id,  # Fijo
            0,  # Last ack'd messageid
            set(),  # Variable
            t,  # No
            threading.Lock(),  # No
            int(rows[1]),  # client batch size
        ]
        with self._activity_log_lock:
            self._activity_log.log_for_client_handler(
                session_id, self._games_queue_name, connection_id, 0, set()
            )

        self._client_middleware.send_multipart(
            connection_id, ["A", session_id], needs_encoding=True
        )

        t.start()

    def process_heartbeat_message(self, message: bytes):
        # if random.randint(0, 9) > 8:
        #     logging.info(f"Bad luck, message of type: {msg_type} dropped")
        #     return
        rows = self._client_middleware.get_row_from_message(message)
        session_id = rows[0]

        logging.debug(f"Received heartbeat from {session_id}")
        client_info = self._clients_info[session_id]
        self._client_middleware.send_multipart(
            connection_id=client_info[CLIENT_INFO_CONNECTION_ID_INDEX],
            message=[
                HEARTBEAT_MESSAGE_TYPE,
                rows[1],
                str(client_info[CLIENT_INFO_LAST_MESSAGE_INDEX]),
            ],  # rows[1] contains heartbeat id, its used for detecting duplicates
            needs_encoding=True,
        )

    def process_message(self, msg_type: str, connection_id, message: bytes):
        # logging.debug(f"Received message of type: {msg_type} | message: {message}")

        if msg_type == DATA_MESSAGE_TYPE:
            self.process_data_message(message)
        elif msg_type == QUERY_RESULTS_REQUEST_MESSAGE_TYPE:
            self.process_query_results_request_message(message)
        elif msg_type == HEARTBEAT_MESSAGE_TYPE:
            self.process_heartbeat_message(message)
        # elif msg_type == SESSION_RESTART_MESSAGE_TYPE:
        #     self.process_session_restart_message(message)
        elif msg_type == NEW_SESSION_REQUEST_MESSAGE_TYPE:
            self.process_new_session_request_message(connection_id, message)
        elif msg_type == SESSION_ACK_MESSAGE_TYPE:
            self.process_session_ack_message(connection_id, message)
        else:
            logging.info(f"Message type: {msg_type} not recognized")

    def handle_clients(self):
        self._middleware.create_queue(name="games")
        self._middleware.create_queue(name="reviews")
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
        storage.delete_directory(f"/tmp/{client_id}")

        client_info = self._clients_info[client_id]
        # storage.delete_files_from_directory(f"/tmp/{client_id}")

        client_info[CLIENT_INFO_TIMER_INDEX].cancel()
        # TODO: Ver que onda si se cae por aca
        del client_info
        logging.info(f"Removed all the data from client: {client_id}")

    def handle_client_timeout(self, client_id):
        self._middleware.execute_from_another_thread(
            functools.partial(self.send_timeout, client_id=client_id)
        )

    def run(self):
        monitor_thread = threading.Thread(target=self._client_monitor.start)
        monitor_thread.start()
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
            # monitor_thread.join()

    def on_message(self, channel, method_frame, header_frame, body):
        rows = self.results_middleware.get_rows_from_message(body)
        logging.info(f"Rows: {rows}")

        client_id = rows[0][0]
        query = method_frame.routing_key

        logging.info(f"received results from queue: {query} from client: {client_id}")

        batch_per_client, client_ends = self._get_batch_per_client_and_ends(rows)
        logging.info(
            f"batch_per_client: {batch_per_client} | client_ends: {client_ends}"
        )

        # Update client info
        storage.save_multiclient_batch(
            dir="/tmp",
            batchs_per_client=batch_per_client,
            file_name=query,
        )

        self._add_finished_query_to_clients(clients=client_ends, query=query)

        channel.basic_ack(
            delivery_tag=method_frame.delivery_tag
        )  # TODO: Mover al middleware?

    def __sigterm_handler(self, sig, frame):
        logging.info("Shutting down Client Handler")
        self._got_sigterm.set()
        self._client_monitor.stop()
        raise SystemExit

    # Given a batch that can possibly contain multiple clients info (only used for query results),
    # returns a dictionary with client_id: batch, and also a set with client_ids, which represents
    # all the ENDs found in the batch (for every query), in order to identify them
    def _get_batch_per_client_and_ends(
        self, records: list[list[str]]
    ) -> Tuple[dict[str, list[str]], set[str]]:
        """Given a batch with query results that must contain [client_id, msg_id, result] where result can be and END too, returns every client with its associated data, and the client_id of the ends if found

        Args:
            records (list[list[str]]): batch with clients query results

        Returns:
            Tuple[dict[str, list[str]], set[str]]: client with its batch, the number of ENDs found per client (this maybe doesn't have too much sense)
        """
        batch_per_client = {}
        clients_ends = set()
        # Get the batch for every client
        for record in records:
            client_id = record[0]
            record = record[1:]

            logging.info(f"Mensge: {record}")
            if len(record) > 1 and record[1] == "END":

                record = [record[0] + record[1], record[1]]  # msg_idEND
                clients_ends.add(client_id)
            if not client_id in batch_per_client:
                batch_per_client[client_id] = []

            batch_per_client[client_id].append(record)
        return (batch_per_client, clients_ends)

    def _add_finished_query_to_clients(self, clients: set[str], query: str):
        logging.info(f"Clients: {clients}")
        for client_id in clients:
            client_info = self._clients_info[client_id]
            logging.info(f"client_info: {client_info}")

            with client_info[CLIENT_INFO_FINISHED_QUERIES_LOCK_INDEX]:
                client_info[CLIENT_INFO_FINISHED_QUERIES_INDEX].add(query)

                logging.info(
                    f"Client: {client_id} | {self._clients_info[client_id][CLIENT_INFO_FINISHED_QUERIES_INDEX]}"
                )
            with self._activity_log_lock:
                self._activity_log.log_for_client_handler(
                    client_id=client_id,
                    queue_name=client_info[CLIENT_INFO_QUEUE_NAME_INDEX],
                    connection_id=client_info[CLIENT_INFO_CONNECTION_ID_INDEX],
                    last_ack_message=client_info[CLIENT_INFO_LAST_MESSAGE_INDEX],
                    finished_querys=client_info[CLIENT_INFO_FINISHED_QUERIES_INDEX],
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
