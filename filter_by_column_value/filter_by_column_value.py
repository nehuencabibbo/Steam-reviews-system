import csv
import sys, os

from common.storage.storage import write_by_range

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import *
from constants import *
from common.protocol.protocol import Protocol
from common.middleware.middleware import Middleware, MiddlewareError
from utils.utils import node_id_to_send_to

import signal
import logging
import langid


class FilterColumnByValue:
    def __init__(
        self,
        protocol: Protocol,
        middleware: Middleware,
        config: Dict[str, Union[str, int]],
    ):
        self._protocol = protocol
        self._middleware = middleware
        self._config = config
        self._got_sigterm = False
        self.e = 0
        self.r = 0
        self.n = 0
        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def start(self):
        # Reciving queues
        self._middleware.create_queue(self._config["RECIVING_QUEUE_NAME"])

        # Forwarding queues
        for i in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
            self._middleware.create_queue(
                f"{i}_{self._config['FORWARDING_QUEUE_NAME']}"
            )

        # Attaching callback functions
        callback = self._middleware.__class__.generate_callback(
            self.__handle_message,
        )
        self._middleware.attach_callback(self._config["RECIVING_QUEUE_NAME"], callback)

        try:
            self._middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)

    def __handle_end_transmission(self, body: List[str]):
        # Si me llego un END...
        # 1) Me fijo si los la cantidad de ids que hay es igual a
        # la cantidad total de instancias de mi mismo que hay.
        # Si es asi => Envio el END a la proxima cola
        # Si no es asi => Checkeo si mi ID esta en la lista
        #     Si es asi => No agrego nada y reencolo
        #     Si no es asi => Agrego mi id a la lista y reencolo
        peers_that_recived_end = body[1:]
        if len(peers_that_recived_end) == int(self._config["INSTANCES_OF_MYSELF"]):
            self.__send_end_transmission_to_all_forwarding_queues()

        else:

            message = [END_TRANSMISSION_MESSAGE]
            if not self._config["NODE_ID"] in peers_that_recived_end:
                peers_that_recived_end.append(self._config["NODE_ID"])

            message += peers_that_recived_end

            self._middleware.publish_message(
                message, self._config["RECIVING_QUEUE_NAME"]
            )

    def _send_last_batch_to_fowarding_queues(self):
        for i in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
            queue_name = f"{i}_{self._config['FORWARDING_QUEUE_NAME']}"
            self._middleware.publish_batch(queue_name)

    def __send_end_transmission_to_all_forwarding_queues(self):
        # TODO: Verify that EVERY queue is started at 0
        for i in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):

            self._middleware.send_end(
                queue=f"{i}_{self._config['FORWARDING_QUEUE_NAME']}"
            )

    def __handle_message(self, delivery_tag: int, body: bytes):
        body = self._middleware.get_rows_from_message(body)
        for message in body:
            message = [value.strip() for value in message]
            logging.debug(f"Recived message: {message}")

            if message[0] == END_TRANSMISSION_MESSAGE:
                self._send_last_batch_to_fowarding_queues()
                self.__handle_end_transmission(message)
                self._middleware.ack(delivery_tag)

                return

            self.__filter_according_to_criteria(message)

        self._middleware.ack(delivery_tag)

    def __filter_according_to_criteria(self, body: List[str]):
        column_to_use = body[self._config["COLUMN_NUMBER_TO_USE"]]
        value_to_filter_by = self._config["VALUE_TO_FILTER_BY"]
        criteria = self._config["CRITERIA"]
        self.r += 1
        logging.debug(f"amount of reviews: {self.r}")

        if criteria == EQUAL_CRITERIA_KEYWORD:
            if column_to_use == value_to_filter_by:
                self.__send_message(body)

        elif criteria == GRATER_THAN_CRITERIA_KEYWORD:
            try:
                column_to_use = int(column_to_use)
                value_to_filter_by = int(value_to_filter_by)
            except ValueError as e:
                logging.debug(f"Failed integer conversion: {e}")

            if column_to_use > value_to_filter_by:
                self.__send_message(body)

        elif criteria == CONTAINS_CRITERIA_KEYWORD:
            if value_to_filter_by in column_to_use.lower():
                self.__send_message(body)

        elif criteria == LANGUAGE_CRITERIA_KEYWORD:
            detected_language, _ = langid.classify(column_to_use)
            if detected_language == value_to_filter_by.lower():
                with open("./english_output.csv", "a") as f:
                    writer = csv.writer(f)
                    writer.writerow([body[0], body[1], detected_language])
                logging.debug(f"English detected: {body}")
                self.e += 1
                logging.debug(f"amount of english: {self.e}")
                self.__send_message(body)
            else:
                with open("./no_english_output.csv", "a") as f:
                    writer = csv.writer(f)
                    writer.writerow([body[0], body[1], detected_language])

        elif criteria == EQUAL_FLOAT_CRITERIA_KEYWORD:
            try:
                column_to_use = float(column_to_use)
                value_to_filter_by = float(value_to_filter_by)
            except ValueError as e:
                logging.debug(f"Failed float conversion: {e}")

            logging.debug(f"Comparing: {column_to_use} with {value_to_filter_by}")
            if column_to_use == value_to_filter_by:
                self.n += 1
                logging.debug(f"Negatives sent: {self.n}")
                self.__send_message(body)
        else:
            raise Exception(f"Unkown cirteria: {criteria}")

    def __filter_columns(self, columns_to_keep: List[int], data: List[str]):
        # No filter needed
        if len(columns_to_keep) == 1 and columns_to_keep[0] == -1:
            return data

        return [data[i] for i in columns_to_keep]

    def __send_message(self, message: List[str]):
        """
        Do not use to send END message as it is handled differently.
        """
        message = self.__filter_columns(self._config["COLUMNS_TO_KEEP"], message)
        # encoded_message = self._protocol.encode(message)

        # TODO: Change with app id when available
        # for i in range(self._config["AMOUNT_OF_FORWARDING_QUEUES"]):
        node_id = node_id_to_send_to(
            "1", message[APP_ID], self._config["AMOUNT_OF_FORWARDING_QUEUES"]
        )
        logging.debug(
            f'Sending message: {message} to queue: {node_id}_{self._config["FORWARDING_QUEUE_NAME"]}'
        )
        self._middleware.publish(
            message, f'{node_id}_{self._config["FORWARDING_QUEUE_NAME"]}'
        )

    def __signal_handler(self, sig, frame):
        logging.debug("Gracefully shutting down...")
        self._got_sigterm = True
        self._middleware.shutdown()


# 1047 + 12554 + 13351 = 26952 -> Drop nulls a negative (Correcto)
# Negative a english
# 2733 + 2783 + 2774 + 2798 = 11088-> english totales (esto en el kaggle da despues de el filtro de negativos)


# 1155685 / 150/s = 7704.56667 segundos


#  1 - 60
#      - 42780


# received, english
# 2788 + 2760 + 2719 + 2814

# 2648 + 2558 + 2643 + 2628


# enviados (desde negtive): 5536 + 5549
# recibidos: 2748 + 2784 + 2778 + 2771
