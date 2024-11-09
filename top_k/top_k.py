import logging
import signal
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import (
    read_sorted_file,
    add_batch_to_sorted_file_per_client,
    delete_directory,
)
from common.protocol.protocol import Protocol

END_TRANSMISSION_MESSAGE = "END"
SESSION_TIMEOUT_MESSAGE = "TIMEOUT"


class TopK:
    def __init__(self, middleware: Middleware, config: dict[str, str]):
        self.__middleware = middleware
        self.__total_ends_received_per_client = {}
        self.__total_timeouts_received_per_client = {}
        self._got_sigterm = False
        self._node_id = config["NODE_ID"]
        self._input_top_k_queue_name = config["INPUT_TOP_K_QUEUE_NAME"]
        self._amount_of_receiving_queues = config["AMOUNT_OF_RECEIVING_QUEUES"]
        self._output_top_k_queue_name = config["OUTPUT_TOP_K_QUEUE_NAME"]
        self._k = config["K"]

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

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
                    self.__middleware.send_end(
                        queue=forwarding_queue_name,
                        end_message=[session_id, SESSION_TIMEOUT_MESSAGE],
                    )

            self.__middleware.ack(delivery_tag)
            return

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
                self.__send_top(forwarding_queue_name, client_id=client_id)

                end_message = [client_id, END_TRANSMISSION_MESSAGE]
                self.__middleware.send_end(
                    queue=forwarding_queue_name,
                    end_message=end_message,
                )

                client_storage_dir = f"/tmp/{client_id}"
                self._clear_client_data(client_id, client_storage_dir)

            self.__middleware.ack(delivery_tag)

            return

        try:
            add_batch_to_sorted_file_per_client(
                "tmp", body, ascending=False, limit=int(self._k)
            )

        except ValueError as e:
            logging.error(
                f"An error has occurred. {e}",
            )

        self.__middleware.ack(delivery_tag)

    def __send_top(self, forwarding_queue_name, client_id):
        for record in read_sorted_file(f"tmp/{client_id}"):
            # if not "Q" in forwarding_queue_name:
            record.insert(0, client_id)
            self.__middleware.publish(record, forwarding_queue_name, "")

        self.__middleware.publish_batch(forwarding_queue_name)
        logging.debug(f"Top sent to queue: {forwarding_queue_name}")

    def _clear_client_data(self, client_id: str, storage_dir: str):

        if not delete_directory(storage_dir):
            logging.debug(f"Couldn't delete directory: {storage_dir}")
        else:
            logging.debug(f"Deleted directory: {storage_dir}")

        try:
            self.__total_timeouts_received_per_client.pop(
                client_id
            )  # removed timeout count for the client
        except KeyError:
            logging.debug("No session found with id: {session_id}. Omitting.")

        try:
            self.__total_ends_received_per_client.pop(
                client_id
            )  # removed end count for the client
        except KeyError:
            logging.debug("No session found with id: {session_id}. Omitting.")
