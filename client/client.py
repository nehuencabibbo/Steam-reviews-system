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


class Client:

    def __init__(
        self,
        config: dict,
        middleware: ClientMiddleware,
    ):
        self._middleware = middleware
        self._got_sigterm = False
        self._q4_ends_to_wait_for = 0  # TODO: Change to send a single end message
        self._queries_results_left = {"Q1", "Q2", "Q3", "Q4", "Q5"}

        self._client_id: str = None
        self._query_results = {}
        self.__find_and_set_csv_field_size_limit()
        self._next_message_id = 0

        self._server_ip = config.get("SERVER_IP")
        self._server_port = config.get("SERVER_PORT")
        self._game_file_path = config.get("GAME_FILE_PATH")
        self._reviews_file_path = config.get("REVIEWS_FILE_PATH")
        self._sending_wait_time = config.get("SENDING_WAIT_TIME")
        self._check_status_after_messages = 99
        self._last_read_line = 0
        self._start_line = 0
        self._last_acked_message = None

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

        try:
            self.__send_start_of_new_session()
            # self._middleware.disconnect(self._server_ip, self._server_port)
            # self._middleware.close_socket(0)
            # self._middleware.create_socket(zmq.DEALER)
            # self._middleware.connect_to(ip=self._server_ip, port=self._server_port)
            # self._middleware.set_session_id(self._session_id)
            try:
                self.__send_file(self._game_file_path)
            except ConnectionError:
                self.__send_file(self._game_file_path, must_sync=True)
            try:
                self.__send_file(self._reviews_file_path, must_check_status=True)
            except ConnectionError:
                self.__send_file(
                    self._reviews_file_path, must_sync=True, must_check_status=True
                )

            self.__get_results()
        except zmq.error.ZMQError:
            if not self._got_sigterm:
                raise

    def __send_start_of_new_session(self):
        self._middleware.send_message(["N"])
        self._middleware.register_for_pollin()
        while True:
            logging.info("Pollin")
            if self._middleware.has_message():
                res = self._middleware.recv_batch()
                self._session_id = res[0][0]
                logging.info(f"Received id: {self._session_id}")

                return

    def __send_file(
        self, file_path, must_sync: bool = False, must_check_status: bool = False
    ):
        # self._last_read_line = 0
        is_sync = must_sync

        with open(file_path, "r") as file:
            reader = csv.reader(file)
            next(reader, None)  # skip header
            for row in reader:
                if self._got_sigterm:
                    return
                if must_sync:
                    if not is_sync and self._last_read_line == self._next_message_id:
                        # Starting line was found
                        is_sync = True
                        self._next_message_id = self._last_read_line
                    if not is_sync:
                        # Actual line is behind the starting line
                        self._last_read_line += 1
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

                self._next_message_id += 1

                time.sleep(self._sending_wait_time)
        logging.debug("Sending file end")

        if self._last_read_line >= self._start_line:
            self._middleware.send_end(
                session_id=self._session_id,
                end_message=[str(self._next_message_id), FILE_END_MSG],
            )
            if must_check_status:
                self._check_status()

            self._next_message_id += 1

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

        # for result in results:
        #     self._query_results[query].append(result)  # [1:] to remove client_id

    def __get_results(self):
        logging.info("Waiting for results...")

        while not self._got_sigterm and len(self._queries_results_left) > 0:
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

    def _raise_connection_error(self):
        raise ConnectionError()

    # def _check_status(self):
    #     # C: Continue
    #     self._middleware.send_message(["C", self._session_id])

    #     t = threading.Timer(2.0, self._raise_connection_error)
    #     t.start()

    #     while not self._got_sigterm:
    #         logging.info("Pollin")
    #         if self._middleware.has_message():
    #             res = self._middleware.recv_batch()
    #             last_acked_message = int(res[0][1])

    #             logging.info(f"Received: {res}")
    #             if self._next_message_id == last_acked_message:
    #                 self._last_acked_message = last_acked_message
    #                 return
    #             else:
    #                 logging.error(
    #                     f"Expected: {self._next_message_id}. got: {last_acked_message}"
    #                 )
    #                 raise ValueError()

    def _check_status(self):
        # H: Heartbeat
        heartbeat_id = str(uuid.uuid4())
        self._middleware.send_message(["H", self._session_id, heartbeat_id])
        t = threading.Timer(30.0, self._raise_connection_error)
        t.start()

        while not self._got_sigterm:
            logging.info("Polling for heartbeat echo")
            if self._middleware.has_message():
                res = self._middleware.recv_batch()
                logging.info(f"Heartbeat res: {res}")
                if res[0][0] == "H" and res[0][1] == heartbeat_id:
                    t.cancel()
                    return

    def __sigterm_handler(self, signal, frame):
        self._got_sigterm = True
        self._middleware.shutdown()
