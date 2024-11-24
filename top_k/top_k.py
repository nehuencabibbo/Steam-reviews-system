import logging
import os
import signal
from common.activity_log.activity_log import ActivityLog
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import (
    read_sorted_file,
    add_batch_to_sorted_file_per_client,
    delete_directory,
    _add_batch_to_sorted_file,
)
from utils.utils import group_batch_by_field
from typing import *

END_TRANSMISSION_MESSAGE = "END"


class TopK:
    def __init__(
        self, middleware: Middleware, config: dict[str, str], activity_log: ActivityLog
    ):
        self.__middleware = middleware
        self.__total_ends_received_per_client = {}
        self._got_sigterm = False
        self._node_id = config["NODE_ID"]
        self._input_top_k_queue_name = config["INPUT_TOP_K_QUEUE_NAME"]
        self._amount_of_receiving_queues = config["AMOUNT_OF_RECEIVING_QUEUES"]
        self._output_top_k_queue_name = config["OUTPUT_TOP_K_QUEUE_NAME"]
        self._k = int(config["K"])
        self._activity_log = activity_log

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

        self.__recover_state()

    def __signal_handler(self, sig, frame):
        logging.debug(f"Gracefully shutting down...")
        self._got_sigterm = True
        # self.__middleware.stop_consuming_gracefully()
        self.__middleware.shutdown()

    def start(self):
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
        try:
            self.__middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self.__middleware.shutdown()

    def __recover_state(self):
        full_file_path, lines = self._activity_log.recover()
        
        if not full_file_path or not lines:
            logging.debug("General log was corrupted, not recovering any state.")
            return

        logging.debug(
            f"Recovering state, {full_file_path} is being processed with batch: "
        )
        for line in lines:
            logging.debug(line)
        dir = full_file_path.rsplit("/", maxsplit=1)[0]

        if not os.path.exists(dir):
            logging.debug(
                (
                    f"Ended up aborting state recovery, as {dir} "
                    f"was cleaned up after receiving END"
                )
            )
            return

        lines = list(map(lambda x: x.rsplit(",", maxsplit=2), lines))
        logging.debug(f"LINEAS: {lines}")
        _add_batch_to_sorted_file(
            dir, lines, self._activity_log, ascending=False, limit=self._k
        )

    def __callback(self, delivery_tag, body, message_type):
        body = self.__middleware.get_rows_from_message(body)
        logging.debug(f"[INPUT GAMES] received: {body}")

        if len(body) == 1 and body[0][1] == END_TRANSMISSION_MESSAGE:
            client_id = body[0][0]
            self.__total_ends_received_per_client[client_id] = (
                self.__total_ends_received_per_client.get(client_id, 0) + 1
            )
            logging.debug("END of games received")

            if (
                self.__total_ends_received_per_client[client_id]
                == self._amount_of_receiving_queues
            ):
                forwarding_queue = self._output_top_k_queue_name
                # Add the client id if its sink node
                forwarding_queue_name = forwarding_queue

                self.__middleware.create_queue(forwarding_queue_name)
                # TODO: HANDELEAR CASO QUE SE ROMPA MID MANDAR COSAS
                self.__send_top(forwarding_queue_name, client_id=client_id)

                end_message = [client_id, END_TRANSMISSION_MESSAGE]
                self.__middleware.send_end(
                    queue=forwarding_queue_name,
                    end_message=end_message,
                )

                client_storage_dir = f"/tmp/{client_id}"
                # TODO: Si se rompe mientras borra la data del cliente tambien hay que
                # handelearlo
                self._clear_client_data(client_id, client_storage_dir)

            self.__middleware.ack(delivery_tag)

            return

        records_per_client = group_batch_by_field(body)
        self.__purge_duplicates(records_per_client)

        try:
            add_batch_to_sorted_file_per_client(
                "tmp",
                records_per_client,
                self._activity_log,
                ascending=False,
                limit=self._k,
            )

        except ValueError as e:
            logging.error(
                f"An error has occurred. {e}",
            )

        self.__middleware.ack(delivery_tag)

    def __purge_duplicates(self, records_per_client: Dict[str, List[List[str]]]):
        # Para cada batch, para cada cliente, se actualiza por completo o no un determinado archivo (firma de
        # la func del storage), si al menos UN msg_id para un cliente ya fue procesado, eso quiere decir que
        # todos los fueron, por lo tanto no hace falta checkear todos

        client_ids_to_remove = []
        for client_id, records in records_per_client.items():
            for (
                msg_id,
                name,
                count,
            ) in records:  # TODO: Por ahi hay alguna forma mejor de hacer esto
                if self._activity_log.is_msg_id_already_processed(client_id, msg_id):
                    client_ids_to_remove.append(client_id)

                break

        for client_id in client_ids_to_remove:
            del records_per_client[client_id]

    def __send_top(self, forwarding_queue_name, client_id):
        NAME = 0
        COUNT = 1
        MSG_ID = 2
        logging.debug("Sending the following top:")
        for record in read_sorted_file(f"tmp/{client_id}"):
            # Si no se lo paso a un agregador, sino que se lo estoy mandando al cliente,
            # le tengo que mandar solo lo que le interesa
            if "Q" in forwarding_queue_name:
                record = [client_id, record[NAME], record[COUNT]]
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
        self.__total_ends_received_per_client.pop(
            client_id
        )  # removed end count for the client
