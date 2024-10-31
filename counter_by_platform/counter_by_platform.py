import signal
import logging
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from common.protocol.protocol import Protocol
from typing import *

END_TRANSMISSION_MESSAGE = "END"

END_TRANSMISSION_MESSAGE_INDEX = 1
END_TRANSMISSION_SESSION_ID = 0

REGULAR_MESSAGE_SESSION_ID = 0
REGULAR_MESSAGE_FIELD_TO_COUNT_BY = 1


class CounterByPlatform:

    def __init__(self, config, middleware: Middleware):
        self._config = config
        self._middleware = middleware
        self._got_sigterm = False
        self._count_dict = {}
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):

        consume_queue_name = (
            f"{self._config['NODE_ID']}_{self._config['CONSUME_QUEUE_SUFIX']}"
        )
        self._middleware.create_queue(consume_queue_name)
        self._middleware.create_queue(self._config["PUBLISH_QUEUE"])

        callback = self._middleware.generate_callback(self.__handle_message)

        self._middleware.attach_callback(consume_queue_name, callback)

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            # TODO: If got_sigterm is showing any error needed?
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self._middleware.shutdown()

    def __handle_message(self, delivery_tag: int, body: List[List[str]]):
        body = self._middleware.get_rows_from_message(body)
        #for message in body:

        logging.debug(f"GOT BATCH: {body}")

        if body[0][END_TRANSMISSION_MESSAGE_INDEX] == END_TRANSMISSION_MESSAGE:
            logging.debug("Recived END transmssion")
            session_id = body[0][END_TRANSMISSION_SESSION_ID]

            self.__send_results(session_id)
            self._middleware.ack(delivery_tag)

            return


        count_per_record_by_client_id = {}
        for record in body:
            client_id = record[REGULAR_MESSAGE_SESSION_ID]
            record_id = record[REGULAR_MESSAGE_FIELD_TO_COUNT_BY]

            if not client_id in count_per_record_by_client_id:
                count_per_record_by_client_id[client_id] = {}

            count_per_record_by_client_id[client_id][record_id] = (
                count_per_record_by_client_id[client_id].get(record_id, 0) + 1
            )

        storage.sum_platform_batch_to_records_per_client(
            self._config["STORAGE_DIR"],
            count_per_record_by_client_id,
        )

        # logging.debug(message)
        # session_id = message[REGULAR_MESSAGE_SESSION_ID]
        # field_to_count = message[REGULAR_MESSAGE_FIELD_TO_COUNT_BY]

        # self.__count(field_to_count, session_id)

        self._middleware.ack(delivery_tag)

    def __send_results(self, session_id: str):
        # TODO: READ FROM STORAGE
        publish_queue = self._config["PUBLISH_QUEUE"]
        self._middleware.create_queue(publish_queue)

        client_dir = f"{self._config['STORAGE_DIR']}/{session_id}"
        reader = storage.read_all_files(client_dir)

        for record in reader:
            logging.debug(f"sending record: {record}")
            if self._got_sigterm:
                #should send everithing so i can ack before closing 
                # or return false so end is not acked and i dont send the results?
                return
            
            self._middleware.publish(
                [session_id, record[0], record[1]], queue_name=publish_queue
            )

        self._middleware.publish_batch(publish_queue)
        self._middleware.send_end(
            queue=publish_queue, end_message=[session_id, END_TRANSMISSION_MESSAGE]
        )
        logging.debug(f"Sent results to queue {publish_queue}")


        # for platform, count in self._count_dict[session_id].items():
        #     logging.debug(f"{platform}, {count}")
        #     if self._got_sigterm:
        #         return

        #     # encoded_msg = self.protocol.encode([key, str(value)])
        #     self._middleware.publish(
        #         [session_id, platform, str(count)], queue_name=publish_queue
        #     )

        # encoded_msg = self.protocol.encode([END_TRANSMISSION_MESSAGE])
        # self.middleware.publish(encoded_msg, queue_name=self.config["PUBLISH_QUEUE"])

    # def __count(self, field_to_count: str, session_id: str):
    #     # TODO: SAVE AND UPDATE ON STORAGE

    #     # session_id = {windows: algo, mac: algo, linux: algo}

    #     if not session_id in self._count_dict:
    #         self._count_dict[session_id] = {}

    #     session_id_field_count = self._count_dict[session_id].get(field_to_count, 0)

    #     self._count_dict[session_id][field_to_count] = session_id_field_count + 1

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        self._middleware.stop_consuming_gracefully()
        # self._middleware.shutdown()
