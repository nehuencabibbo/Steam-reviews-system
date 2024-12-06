import logging
import os
import signal
import threading

from common.activity_log.activity_log import ActivityLog
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import (
    read_sorted_file,
    add_batch_to_sorted_file_per_client,
    delete_directory,
)
from common.watchdog_client.watchdog_client import WatchdogClient

from typing import *

END_TRANSMISSION_MESSAGE = "END"
SESSION_TIMEOUT_MESSAGE = "TIMEOUT"

END_TRANSMISSION_MESSAGE_CLIENT_ID_INDEX = 0
END_TRANSMISSION_MESSAGE_MSG_ID_INDEX = 1
END_TRANSMISSION_MESSAGE_END_INDEX = 2


class TopK:
    def __init__(
        self,
        middleware: Middleware,
        monitor: WatchdogClient,
        config: dict[str, str],
        activity_log: ActivityLog,
    ):
        self.__middleware = middleware
        self._client_monitor = monitor

        self.__total_ends_received_per_client = {}
        self.__total_timeouts_received_per_client = {}
        self._got_sigterm = False
        self._node_id = config["NODE_ID"]
        self._input_top_k_queue_name = config["INPUT_TOP_K_QUEUE_NAME"]
        self._amount_of_receiving_queues = config["AMOUNT_OF_RECEIVING_QUEUES"]
        self._output_top_k_queue_name = config["OUTPUT_TOP_K_QUEUE_NAME"]
        self._k = int(config["K"])
        self._activity_log = activity_log

        self.__total_ends_received_per_client = self._activity_log.recover_ends_state()

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def __signal_handler(self, sig, frame):
        logging.debug(f"Gracefully shutting down...")
        self._got_sigterm = True
        # self.__middleware.stop_consuming_gracefully()
        self.__middleware.shutdown()
        self._client_monitor.stop()

    def start(self):

        monitor_thread = threading.Thread(target=self._client_monitor.start)
        monitor_thread.start()

        self.__middleware.create_queue(
            f"{self._node_id}_{self._input_top_k_queue_name}"
        )

        # # callback, inputq, outputq
        games_callback = self.__middleware.generate_callback(
            self.__callback,
            f"{self._node_id}_{self._input_top_k_queue_name}",
        )

        self.__middleware.attach_callback(
            f"{self._node_id}_{self._input_top_k_queue_name}",
            games_callback,
        )

        self.__resume_publish_if_necesary()
        try:
            self.__middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self.__middleware.shutdown()
            # monitor_thread.join()

    def __resume_publish_if_necesary(self):
        # Si me cai mientras estaba mandando los resultados (me llego el ultimo END)
        # tengo que retomar mandar los resultados
        client_ids_to_clear = []
        for client_id, amount in self.__total_ends_received_per_client.items():
            if amount == self._amount_of_receiving_queues:

                self.__middleware.create_queue(self._output_top_k_queue_name)
                # TODO: HANDELEAR CASO QUE SE ROMPA MID MANDAR COSAS
                logging.debug("Sending top [Restart]")
                self.__send_top(self._output_top_k_queue_name, client_id=client_id)

                end_message = [client_id, self._node_id, END_TRANSMISSION_MESSAGE]
                self.__middleware.send_end(
                    queue=self._output_top_k_queue_name,
                    end_message=end_message,
                )
                client_ids_to_clear.append(client_id)

        for client_id in client_ids_to_clear:
            client_storage_dir = f"/tmp/{client_id}"
            # TODO: Si se rompe mientras borra la data del cliente tambien hay que
            # handelearlo
            self._clear_client_data(client_id, client_storage_dir)
            self._activity_log.remove_client_logs(client_id)

    def __callback(self, delivery_tag, body, message_type):
        body = self.__middleware.get_rows_from_message(body)
        logging.debug(f"[INPUT GAMES] received: {body}")

        if body[0][1] == SESSION_TIMEOUT_MESSAGE:
            session_id = body[0][0]
            logging.info(f"Timeout received for session: {session_id}")

            self.__total_timeouts_received_per_client[session_id] = (
                self.__total_timeouts_received_per_client.get(session_id, 0) + 1
            )

            if (
                self.__total_timeouts_received_per_client[session_id]
                == self._amount_of_receiving_queues
            ):
                client_storage_dir = f"/tmp/{session_id}"
                self._clear_client_data(session_id, client_storage_dir)
                forwarding_queue_name = self._output_top_k_queue_name

                # If it contains Q<x>, then it's an aggregator, shouldn't forward the timeout
                if "Q" not in forwarding_queue_name:
                    self.__middleware.publish_message(
                        [session_id, SESSION_TIMEOUT_MESSAGE],
                        forwarding_queue_name,
                    )

            self.__middleware.ack(delivery_tag)
            return

        if (
            len(body) == 1
            and body[0][END_TRANSMISSION_MESSAGE_END_INDEX] == END_TRANSMISSION_MESSAGE
        ):
            logging.debug("END of games received")
            client_id = body[0][END_TRANSMISSION_MESSAGE_CLIENT_ID_INDEX]
            msg_id = body[0][END_TRANSMISSION_MESSAGE_MSG_ID_INDEX]

            self.__handle_end_transmission(client_id, msg_id)

            self.__middleware.ack(delivery_tag)

            return

        try:
            add_batch_to_sorted_file_per_client(
                "tmp",
                body,
                ascending=False,
                limit=self._k,
            )

        except ValueError as e:
            logging.error(
                f"An error has occurred. {e}",
            )

        self.__middleware.ack(delivery_tag)

    def __handle_end_transmission(self, client_id: str, msg_id: str):
        end_was_duplicated = self._activity_log.log_end(client_id, msg_id)
        if end_was_duplicated:
            return

        self.__total_ends_received_per_client[client_id] = (
            self.__total_ends_received_per_client.get(client_id, 0) + 1
        )

        if (
            self.__total_ends_received_per_client[client_id]
            == self._amount_of_receiving_queues
        ):
            self.__middleware.create_queue(self._output_top_k_queue_name)
            # TODO: HANDELEAR CASO QUE SE ROMPA MID MANDAR COSAS
            logging.debug("Sending top [Handle end transmssion]")
            self.__send_top(self._output_top_k_queue_name, client_id=client_id)

            end_message = [client_id, self._node_id, END_TRANSMISSION_MESSAGE]
            self.__middleware.send_end(
                queue=self._output_top_k_queue_name,
                end_message=end_message,
            )

            client_storage_dir = f"/tmp/{client_id}"
            # TODO: Si se rompe mientras borra la data del cliente tambien hay que
            # handelearlo
            self._clear_client_data(client_id, client_storage_dir)
            self._activity_log.remove_client_logs(client_id)

    def __send_top(self, forwarding_queue_name, client_id):
        NAME = 0
        COUNT = 1
        MSG_ID = 2
        logging.debug("Sending the following top:")
        for record in read_sorted_file(f"tmp/{client_id}"):
            # Si no se lo paso a un agregador, sino que se lo estoy mandando al cliente,
            # le tengo que mandar solo lo que le interesa
            if "Q" in forwarding_queue_name:
                record = [client_id, record[MSG_ID], record[NAME], record[COUNT]]
            # Si se lo paso a un agregador, se lo tengo que pasar en el orden
            # en el que yo mismo lo espero
            else:
                record = [client_id, record[MSG_ID], record[NAME], record[COUNT]]

            logging.debug(record)
            self.__middleware.publish(record, forwarding_queue_name, "")

        self.__middleware.publish_batch(forwarding_queue_name)
        logging.debug(f"Top sent to queue: {forwarding_queue_name}")

    def _clear_client_data(self, client_id: str, storage_dir: str):

        if not delete_directory(storage_dir):
            logging.debug(f"Couldn't delete directory: {storage_dir}")
        else:
            logging.debug(f"Deleted directory: {storage_dir}")

        if client_id in self.__total_ends_received_per_client:
            del self.__total_ends_received_per_client[client_id]
        if client_id in self.__total_timeouts_received_per_client:
            del self.__total_timeouts_received_per_client[client_id]
