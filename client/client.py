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

        self._client_id: str = None

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
        self.__send_file(
            self._config["GAME_FILE_PATH"],
        )
        self.__send_file(
            self._config["REVIEWS_FILE_PATH"],
        )

        self.__get_results()

        # if not self._got_sigterm:
        #     self._middleware.shutdown()

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

    def __get_results(self):

        while True:
            logging.debug("Waiting for results...")
            res = self._middleware.recv_string()
            logging.debug(f"Results: {res}")
            time.sleep(5)
        # for number_of_query in range(1, AMMOUNT_OF_QUERIES + 1):
        #     if self._got_sigterm:
        #         return

        #     queue_name = f"Q{number_of_query}_{self._client_id}"
        #     logging.debug(
        #         (
        #             f"Waiting for results of query {number_of_query}, on queue {queue_name}"
        #         )
        #     )

        #     callback = self._middleware.__class__.generate_callback(
        #         self.__handle_query_result, number_of_query
        #     )

        #     self._middleware.attach_callback(queue_name, callback)
        #     try:
        #         self._middleware.start_consuming()
        #     except MiddlewareError as e:
        #         # TODO: If got_sigterm is showing any error needed?
        #         if not self._got_sigterm:
        #             logging.error(e)

    def __handle_query_result(
        self, delivery_tag: int, body: List[List[str]], query_number: int
    ):

        body = self._middleware.get_rows_from_message(message=body)

        # TODO: Tener handlers aparte en todo caso
        if query_number == 4:
            if len(body) == 1 and body[0][0] == FILE_END_MSG:
                self._q4_ends_to_wait_for += 1
                message, ends_to_wait_for = body[0]

                # TODO: use logging.debug()
                logging.info(
                    f"Need: {ends_to_wait_for} ends, Recived: {self._q4_ends_to_wait_for} ends"
                )
                if self._q4_ends_to_wait_for == int(ends_to_wait_for):
                    self._q4_ends_to_wait_for = 0
                    logging.info(f"Finished reciving q{query_number}")
                    self._middleware.stop_consuming()
                    self._middleware.ack(delivery_tag)
                    return

        for message in body:
            if len(message) == 1 and message[0] == FILE_END_MSG:
                self._middleware.stop_consuming()
                self._middleware.ack(delivery_tag)
                logging.info(f"Finished reciving q{query_number}")
                return

            logging.info(f"Q{query_number} result: {message}")

        self._middleware.ack(delivery_tag)

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        self._middleware.shutdown()
