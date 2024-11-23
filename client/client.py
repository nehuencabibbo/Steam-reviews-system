import csv
import threading
import time
import signal
import logging
import uuid

import pika
import zmq
from common.client_middleware.client_middleware import ClientMiddleware
from common.middleware.middleware import Middleware, MiddlewareError

from typing import *

FILE_END_MSG = "END"
AMMOUNT_OF_QUERIES = 5
MAX_HEARTBEAT_RETRIES = 5
MAX_RESTART_SESSION_RETRIES = 8
MAX_NEW_SESSION_RETRIES = 3
MAX_SESSION_ACKS_RETRIES = 5


class Client:

    def __init__(
        self,
        config: dict,
        middleware: ClientMiddleware,
    ):
        self._middleware = middleware
        self._q4_ends_to_wait_for = 0  # TODO: Change to send a single end message
        self._queries_results_left = {"Q1", "Q2", "Q3", "Q4", "Q5"}

        self._client_id: str = None
        self._query_results = {}
        self.__find_and_set_csv_field_size_limit()
        self._next_message_id = 0
        self._session_id = None
        self._server_ip = config.get("SERVER_IP")
        self._server_port = config.get("SERVER_PORT")
        self._game_file_path = config.get("GAME_FILE_PATH")
        self._reviews_file_path = config.get("REVIEWS_FILE_PATH")
        self._sending_wait_time = config.get("SENDING_WAIT_TIME")
        self._check_status_after_messages = 99
        self._last_acked_message = None
        self._heartbeat_timeout = threading.Event()
        self._restart_session_timeout = threading.Event()
        self._has_restarted = False

        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    @staticmethod
    def __find_and_set_csv_field_size_limit():
        """
        Taken from:
        https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
        """
        max_int = 4294967296  # 2^32 - 1 -> Max size supported by our protocol
        while True:
            try:
                csv.field_size_limit(max_int)
                break
            except OverflowError:
                max_int = int(max_int / 10)

    def run(self):
        self._middleware.create_socket(zmq.DEALER)
        self._middleware.connect_to(ip=self._server_ip, port=self._server_port)
        self._middleware.register_for_pollin()

        try:
            self.__send_start_of_new_session()
            # self._middleware.disconnect(self._server_ip, self._server_port)
            # self._middleware.close_socket(0)
            # self._middleware.create_socket(zmq.DEALER)
            # self._middleware.connect_to(ip=self._server_ip, port=self._server_port)
            # self._middleware.set_session_id(self._session_id)

            self.__try_send_file(self._game_file_path)
            self.__try_send_file(self._reviews_file_path, must_check_status=True)
            self.__get_results()

        except zmq.error.ZMQError as e:
            logging.info(f"ZMQ Error: {e}")
        except ConnectionError:
            logging.error("Haven't been able to stablish a session")
        except SystemExit:
            logging.info("Graceful shutdown")

    def __send_start_of_new_session(self, timeout: float = 2.0, retry_number: int = 0):
        # Req session_id
        # Get session_id from the message sent (wait)
        # Send an "A" (ACK) of that session id
        # Wait for the server to "A" (ACK) the previous ACK (wait)
        # Successfully connected

        new_session_req_timeout = threading.Event()
        t = threading.Timer(timeout, self.set_event, args=(new_session_req_timeout,))

        self._middleware.send_message(["N"])
        t.start()

        while not new_session_req_timeout.is_set():
            logging.debug("Pollin")
            if not self._middleware.has_message():
                continue

            res = self._middleware.recv_batch()

            if res[0][0] != "N":
                continue

            t.cancel()
            self.__ack_session(res[0][1])
            if self._session_id:
                return

        retry_number += 1
        if retry_number == MAX_NEW_SESSION_RETRIES:
            logging.info(
                f"Max number of retries reached while trying to get a session: {MAX_NEW_SESSION_RETRIES}. Aborting."
            )
            raise ConnectionError()

        logging.error(
            f"New session timeout, retrying with timeout: {timeout * 2} | Retry number: {retry_number}"
        )
        self.__send_start_of_new_session(timeout=timeout * 2, retry_number=retry_number)

    def __ack_session(
        self, session_id_candidate: str, timeout: float = 2.0, retry_number: int = 0
    ):
        ack_session_req_timeout = threading.Event()
        t = threading.Timer(timeout, self.set_event, args=(ack_session_req_timeout,))

        self._middleware.send_message(["A", session_id_candidate])
        t.start()

        while not ack_session_req_timeout.is_set():
            logging.debug("Pollin")
            if not self._middleware.has_message():
                continue

            res = self._middleware.recv_batch()
            if res[0][0] == "A" and res[0][1] == session_id_candidate:
                # logging.info(f"Received id: {self._session_id}")
                t.cancel()
                self._session_id = res[0][1]
                logging.info(f"Session established: {self._session_id}")
                return

        retry_number += 1
        if retry_number == MAX_SESSION_ACKS_RETRIES:
            logging.info(
                f"Max number of retries reached while trying to get ack: {MAX_SESSION_ACKS_RETRIES}. Aborting."
            )
            raise ConnectionError()

        logging.error(
            f"Session ack timeout, retrying with timeout: {timeout * 2} | Retry number: {retry_number}"
        )
        self.__ack_session(
            session_id_candidate, timeout=timeout * 2, retry_number=retry_number
        )

    def __try_send_file(
        self,
        file_path: str,
        must_sync: bool = False,
        must_check_status: bool = False,
    ):
        """_summary_

        Args:
            file_path (str): path to input file
            must_sync (bool, optional): if there has been a desconnection, must_sync will be true, representing it has to reach the last_acked message to continue. Defaults to False.
            must_check_status (bool, optional): if true, it will make a heartbeat request after to check if the server is alive and processing messages. Defaults to False.
        """
        initial_next_message_id = self._next_message_id
        while True:
            # Restartea -> actualiza last acked message
            # next_message -> 0
            # hasta que next_message no llegue a last acked no parar
            is_ok = self.__send_file(file_path, must_sync, must_check_status)

            if is_ok:
                break

            # Failed to send, must sync with last_acked_message
            must_sync = True
            self._next_message_id = initial_next_message_id

    # The bool returned represents if there was a session restart and the file must be sent again
    # from a certain point
    def __send_file(
        self, file_path: str, must_sync: bool = False, must_check_status: bool = False
    ) -> bool:
        """_summary_

        Args:
            file_path (str): path to input file
            must_sync (bool, optional): if there has been a desconnection, must_sync will be true, representing it has to reach the last_acked message to continue. Defaults to False.
            must_check_status (bool, optional): if true, it will make a heartbeat request after everything has been sent to check if the server is alive and processing messages. Defaults to False.

        Returns:
            bool: True if there has been a session restart. False if everything went okay.
        """
        with open(file_path, "r") as file:
            reader = csv.reader(file)
            next(reader, None)  # skip header
            for row in reader:
                if must_sync:
                    if self._next_message_id == self._last_acked_message:
                        # Starting line was found
                        must_sync = False  # in order to avoid all the additional checks
                        self._next_message_id = self._last_acked_message

                    self._next_message_id += 1
                    continue

                logging.debug(f"Sending appID {row[0]}")
                row.insert(0, str(self._next_message_id))

                self._middleware.send(row, session_id=self._session_id)

                if self._next_message_id % self._check_status_after_messages == 0:
                    # If we dont send_batch, the later comparison will be incorrect
                    self._middleware.send_batch(
                        message_type="D", session_id=self._session_id
                    )

                    self._check_status()

                    if self._has_restarted:
                        self._has_restarted = False
                        return False

                self._next_message_id += 1

                time.sleep(self._sending_wait_time)
        logging.debug("Sending file end")

        if not self._last_acked_message or (
            self._last_acked_message
            and self._next_message_id >= self._last_acked_message
        ):
            self._middleware.send_end(
                session_id=self._session_id,
                end_message=[str(self._next_message_id), FILE_END_MSG],
            )
            if must_check_status:
                self._check_status()

            self._next_message_id += 1

        return True

    def __print_results_for_query(self, query):
        for result in self._query_results[query]:
            logging.info(f"{query} result: {result}")

        logging.info(f"Finished {query}")

    def __handle_query_result(self, results):
        if len(results) == 0:
            return

        query = results.pop(-1)[0]

        if query not in self._query_results:
            self._query_results[query] = []

        for result in results:
            if result[0] == "END":
                logging.info(f"Results for query: {query}: ")
                self.__print_results_for_query(query)
                self._queries_results_left.remove(query)
                return
            self._query_results[query].append(result)

    def __get_results(self):
        logging.info("Waiting for results...")

        while len(self._queries_results_left) > 0:
            logging.info("Pollin results")
            if self._middleware.has_message():
                res = self._middleware.recv_batch()
                self.__handle_query_result(res)
                continue

            logging.info("Requesting results")
            for q in self._queries_results_left:
                self._middleware.send(
                    [q], message_type="Q", session_id=self._session_id
                )
                self._middleware.send_batch(
                    message_type="Q", session_id=self._session_id
                )

    def _set_heartbeat_timeout(self):
        self._heartbeat_timeout.set()

    def _set_restart_session_timeout(self):
        self._restart_session_timeout.set()

    def set_event(self, event):
        event.set()

    def _restart_session(self, timeout: float = 0.5, retry_number: int = 0):
        """_summary_

        Args:
            timeout (float, optional): max time awaited for the timeout to take place. Defaults to 2.0.
            retry_number (int, optional): number of previous retries. Defaults to 0.

        Raises:
            ConnectionError: if MAX_RESTART_SESSION_RETRIES has been reached. It represents that after multiple tries, the server is not working.
        """
        logging.info(f"Trying to restart session: {self._session_id}")
        # C: Continue
        restart_session_id = str(uuid.uuid4())

        self._middleware.send_message(["C", self._session_id, restart_session_id])

        t = threading.Timer(timeout, self._set_restart_session_timeout)
        t.start()

        while not self._restart_session_timeout.is_set():
            logging.info("Pollin")
            if not self._middleware.has_message():
                continue

            # Its blocked polling, so the timeout could have happened in between
            if self._restart_session_timeout.is_set():
                break

            res = self._middleware.recv_batch()
            logging.info(f"Restar session received: {res}")

            if res[0][0] == "C" and res[0][1] == restart_session_id:
                t.cancel()
                self._last_acked_message = int(res[0][2])
                self._has_restarted = True
                return

        # Retry logic
        self._restart_session_timeout.clear()

        retry_number += 1
        if retry_number == MAX_RESTART_SESSION_RETRIES:
            logging.info(
                f"Max number of retries reached: {MAX_RESTART_SESSION_RETRIES}. Aborting."
            )
            raise ConnectionError()

        logging.error(
            f"Restar session timeout, retrying with timeout: {timeout * 2} | Retry number: {retry_number}"
        )
        self._restart_session(timeout=timeout * 2, retry_number=retry_number)

    def _check_status(self, timeout: float = 0.5, retry_number: int = 0):
        """_summary_

        Args:
            timeout (float, optional): max time awaited for the timeout to take place. Defaults to 2.0.
            retry_number (int, optional): number of previous retries. Defaults to 0.
        """
        # H: Heartbeat
        heartbeat_id = str(uuid.uuid4())
        self._middleware.send_message(["H", self._session_id, heartbeat_id])
        t = threading.Timer(timeout, self._set_heartbeat_timeout)
        t.start()

        while not self._heartbeat_timeout.is_set():
            logging.debug("Polling for heartbeat echo")
            if not self._middleware.has_message():
                continue

            if self._heartbeat_timeout.is_set():
                break

            res = self._middleware.recv_batch()
            logging.info(f"Heartbeat res: {res}")
            if res[0][0] == "H" and res[0][1] == heartbeat_id:
                t.cancel()
                return

        self._heartbeat_timeout.clear()
        retry_number += 1

        if retry_number == MAX_HEARTBEAT_RETRIES:
            self._restart_session()
            return

        logging.error(
            f"Heartbeat timeout, retrying with timeout: {timeout * 2} | Retry number: {retry_number}"
        )
        self._check_status(timeout=timeout * 2, retry_number=retry_number)

    def __sigterm_handler(self, signal, frame):
        self._middleware.shutdown()
        raise SystemExit()


# Starts sending file
# After a few messages, starts a heartbeat which must be answered
# If heartbeat is answered correctly -> continues sending
# If not -> retires up to MAX_HEARTBEAT_RETRIES
# If MAX_HEARTBEAT_RETRIES was reached, starts trying to restart session
# Restart session will send a message to ask for the last acked message
# If the server answers correctly, it will resume the session
# It will retry up to MAX_RESTART_SESSION_RETIRES
# If it gets no answer, an error will be raised an the session will finish
