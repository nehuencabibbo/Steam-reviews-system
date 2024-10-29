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


class Client:

    def __init__(
        self,
        config: dict,
        middleware: ClientMiddleware,
    ):
        self._config = config
        self._middleware = middleware
        # self._client_middleware = client_middleware
        self._got_sigterm = False
        self._q4_ends_to_wait_for = 0  # TODO: Cambiar esto de alguna forma para que se envie un unico end, esta horrible asi
        self._amount_of_queries_received = 0

        self._client_id: str = None
        self._query_results = {}
        self.__find_and_set_csv_field_size_limit()
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

    # def __create_queues(self, client_id: str):
    #     # Forwarding data queues
    #     self._middleware.create_queue(f'{self._config["GAMES_QUEUE"]}_{client_id}')
    #     self._middleware.create_queue(f'{self._config["REVIEWS_QUEUE"]}_{client_id}')

    #     # Queues for reciving results
    #     for i in range(1, AMMOUNT_OF_QUERIES + 1):
    #         queue_name = f"Q{i}_{client_id}"
    #         self._middleware.create_queue(queue_name)

    def run(self):
        self._middleware.create_socket(zmq.DEALER)
        self._middleware.connect_to(
            ip=self._config["SERVER_IP"], port=self._config["SERVER_PORT"]
        )
        # self._middleware.send_string("INIT")
        # response = self._middleware.recv_string()

        # logging.debug(f"Received response id: {response}")
        # self._client_id = client_id
        # self.__create_queues(client_id)

        # if response == "OK":
        try:
            self.__send_file(
                self._config["GAME_FILE_PATH"],
            )
            self.__send_file(
                self._config["REVIEWS_FILE_PATH"],
            )

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
                logging.debug(f"Sending appID {row[0]}")

                self._middleware.send(row)
                time.sleep(self._config["SENDING_WAIT_TIME"])

        logging.debug("Sending file end")
        # self._middleware.publish_batch(queue_name)
        self._middleware.send_end()

    def __print_results_for_query(self, query):
        for result in self._query_results[query]:
            logging.info(f"{query} result: {result}")

        logging.info(f"Finished {query}")

    def __handle_query_result(self, results):
        if len(results) == 0:
            return

        query = results.pop(-1)[0]

        if not query in self._query_results.keys():
            self._query_results[query] = []

        if results[0][1] == "END":
            logging.info(f"Results for query: {query}: ")
            self.__print_results_for_query(query)
            return

        for result in results:
            self._query_results[query].append(
                result[1:]
            )  # [1:] in order to remove client_id

    def __get_results(self):
        # TODO: handle sigterm
        logging.info("Waiting for results...")
        self._middleware.register_for_pollin()
        while not self._got_sigterm:
            logging.debug("Polling for results")
            if self._middleware.has_message():
                res = self._middleware.recv_batch()
                self.__handle_query_result(res)

    def __sigterm_handler(self, signal, frame):
        self._got_sigterm = True
        self._middleware.shutdown()
