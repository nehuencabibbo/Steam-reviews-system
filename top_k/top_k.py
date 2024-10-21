import logging
import signal
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage.storage import (
    add_to_top,
    read_top,
    delete_directory,
    add_batch_to_sorted_file,
    read_sorted_file,
)
from common.protocol.protocol import Protocol

END_TRANSMISSION_MESSAGE = "END"


class TopK:
    def __init__(
        self, protocol: Protocol, middleware: Middleware, config: dict[str, str]
    ):
        self.__protocol = protocol
        self.__middleware = middleware
        self.__config = config
        self.__total_ends_received = 0
        self._got_sigterm = False

        signal.signal(signal.SIGINT, self.__signal_handler)
        signal.signal(signal.SIGTERM, self.__signal_handler)

    def __signal_handler(self, sig, frame):
        logging.debug(f"Gracefully shutting down...")
        self._got_sigterm = True
        self.__middleware.shutdown()

    def start(self):
        self.__middleware.create_queue(
            f"{self.__config['NODE_ID']}_{self.__config['INPUT_TOP_K_QUEUE_NAME']}"
        )

        # self.__middleware.create_queue(self.__config["OUTPUT_TOP_K_QUEUE_NAME"])

        # # callback, inputq, outputq
        games_callback = self.__middleware.generate_callback(
            self.__callback,
            f"{self.__config['NODE_ID']}_{self.__config['INPUT_TOP_K_QUEUE_NAME']}",
            # self.__config["OUTPUT_TOP_K_QUEUE_NAME"],
        )

        self.__middleware.attach_callback(
            f"{self.__config['NODE_ID']}_{self.__config['INPUT_TOP_K_QUEUE_NAME']}",
            games_callback,
        )
        try:
            self.__middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)

    def __callback(self, delivery_tag, body, message_type):

        body = self.__middleware.get_rows_from_message(body)
        logging.debug(f"[INPUT GAMES] received: {body}")

        if len(body) == 1 and body[0][1] == END_TRANSMISSION_MESSAGE:
            self.__total_ends_received += 1
            logging.debug("END of games received")
            logging.debug(
                f"Amount of ends received up to now: {self.__total_ends_received} | Expecting: {self.__config['AMOUNT_OF_RECEIVING_QUEUES']}"
            )

            if (
                self.__total_ends_received
                == self.__config["AMOUNT_OF_RECEIVING_QUEUES"]
            ):
                client_id = body[0][0]
                forwarding_queue = self.__config["OUTPUT_TOP_K_QUEUE_NAME"]
                # Add the client id if its sink node
                forwarding_queue_name = (
                    f"{forwarding_queue}_{client_id}"
                    if "Q" in forwarding_queue
                    else forwarding_queue
                )

                self.__middleware.create_queue(forwarding_queue_name)
                self.__send_top(forwarding_queue_name, client_id=client_id)
                self.__middleware.send_end(
                    queue=forwarding_queue_name,
                    end_message=[client_id, END_TRANSMISSION_MESSAGE],
                )

            self.__middleware.ack(delivery_tag)

            # if not delete_directory('/tmp'):
            #     logging.debug(f"Couldn't delete directory: {'/tmp'}")
            # else:
            #     logging.debug(f"Deleted directory: {'/tmp'}")

            return

        try:
            # TODO: optimize
            for record in body:
                client_id = record.pop(0)
                storage_dir = f"/tmp/{client_id}"

                add_batch_to_sorted_file(
                    storage_dir,
                    [record],
                    ascending=False,
                    limit=int(self.__config["K"]),
                )
        except ValueError as e:
            logging.error(
                f"An error has occurred. {e}",
            )

        self.__middleware.ack(delivery_tag)

    def __send_top(self, forwarding_queue_name, client_id):
        for record in read_sorted_file(f"tmp/{client_id}"):
            if not "Q" in forwarding_queue_name:
                record.insert(0, client_id)
            self.__middleware.publish(record, forwarding_queue_name, "")

        self.__middleware.publish_batch(forwarding_queue_name)
        logging.debug(f"Top sent to queue: {forwarding_queue_name}")
