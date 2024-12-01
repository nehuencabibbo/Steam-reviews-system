import csv
import time
import signal
import logging

import pika
import zmq
from common.client_middleware.client_middleware import ClientMiddleware
from common.middleware.middleware import Middleware, MiddlewareError

from typing import *

FILE_END_MSG = "END"
AMMOUNT_OF_QUERIES = 5

END_TRANSMISSION_END_INDEX = 2


class Client:

    def __init__(
        self,
        config: dict,
        middleware: ClientMiddleware,
    ):
        self._middleware = middleware
        self._got_sigterm = False
        self._q4_ends_to_wait_for = 0  # TODO: Change to send a single end message
        self._amount_of_queries_received = 0

        self._client_id: str = None
        self._query_results = {}
        self.__find_and_set_csv_field_size_limit()

        self._server_ip = config.get("SERVER_IP")
        self._server_port = config.get("SERVER_PORT")
        self._game_file_path = config.get("GAME_FILE_PATH")
        self._reviews_file_path = config.get("REVIEWS_FILE_PATH")
        self._sending_wait_time = config.get("SENDING_WAIT_TIME")

        # TODO: Encapsular esto en el middleware
        self._msg_id = 0 

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
            self.__send_file(self._game_file_path)
            self.__send_file(self._reviews_file_path)
            self.__get_results()
        except zmq.error.ZMQError:
            if not self._got_sigterm:
                raise

    def __send_file(self, file_path):
        with open(file_path, "r") as file:
            reader = csv.reader(file)
            next(reader, None)  # skip header
            for row in reader:
                if self._got_sigterm:
                    return
                # logging.debug(f"Sending appID {row[0]}")
                
                self._middleware.send([str(self._msg_id)] + row)
                self._msg_id += 1
                time.sleep(self._sending_wait_time)
                time.sleep(0.02)
                
        logging.debug("Sending file end")
        self._middleware.send_end(str(self._msg_id))

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

        if results[0][END_TRANSMISSION_END_INDEX] == "END":
            logging.info(f"Results for query: {query}: ")
            self.__print_results_for_query(query)
            self._amount_of_queries_received += 1
            return

        for result in results:
            self._query_results[query].append(result[1:])  # [1:] to remove client_id

    def __get_results(self):
        logging.info("Waiting for results...")
        self._middleware.register_for_pollin()
        while (
            not self._got_sigterm
            and self._amount_of_queries_received < AMMOUNT_OF_QUERIES
        ):
            logging.debug("Polling for results")
            if self._middleware.has_message():
                res = self._middleware.recv_batch()
                self.__handle_query_result(res)

    def __sigterm_handler(self, signal, frame):
        self._got_sigterm = True
        self._middleware.shutdown()
