import csv
import os
import signal
import logging
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from common.protocol.protocol import Protocol
from common.activity_log.activity_log import ActivityLog
from typing import *
from utils.utils import group_msg_ids_per_client_by_field

END_TRANSMISSION_MESSAGE = "END"

END_TRANSMISSION_MSG_ID_INDEX = 1
END_TRANSMISSION_CLIENT_ID = 0
END_TRANSMISSION_END_INDEX = 2

REGULAR_MESSAGE_CLIENT_ID = 0
REGULAR_MESSAGE_MSG_ID= 1
REGULAR_MESSAGE_FIELD_TO_COUNT_BY = 2


class CounterByPlatform:

    def __init__(self, config, middleware: Middleware, activity_log: ActivityLog):
        self._middleware = middleware
        self._got_sigterm = False
        self._activity_log = activity_log

        # Assigning config values to instance attributes
        self.node_id = config["NODE_ID"]
        self.consume_queue_suffix = config["CONSUME_QUEUE_SUFIX"]
        self.publish_queue = config["PUBLISH_QUEUE"]
        self.storage_dir = config["STORAGE_DIR"]

        signal.signal(signal.SIGTERM, self.__sigterm_handler)

        self.__recover_state()
        

    def run(self):
        consume_queue_name = f"{self.node_id}_{self.consume_queue_suffix}"

        self._middleware.create_queue(consume_queue_name)
        self._middleware.create_queue(self.publish_queue)

        callback = self._middleware.generate_callback(self.__handle_message)
        self._middleware.attach_callback(consume_queue_name, callback)

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self._middleware.shutdown()

    def __recover_state(self):
        full_file_path, file_state = self._activity_log.recover()
        if not full_file_path or not file_state: 
            logging.debug('General log was corrupted, not recovering any state.')
            return 
        
        logging.debug(f'Recovering state, overriding {full_file_path} with: ')
        for line in file_state:
            logging.debug(line)

        dir, file_name = full_file_path.rsplit('/', maxsplit=1)
        if not os.path.exists(dir):
            logging.debug((
                f'Ended up aborting state recovery, as {dir} '
                f'was cleaned up after receiving END'
            ))
            return 

        # TODO: Encapsular en el storage esto para que no haga falta
        # saber de aca que s eusa csv
        temp_file = os.path.join(dir, f"temp_{file_name}")
        with open(temp_file, mode='w', newline='') as temp:
            writer = csv.writer(temp)
            for line in file_state:
                writer.writerow(line.split(','))

        os.replace(temp_file, full_file_path)

    def __handle_message(self, delivery_tag: int, body: List[List[str]]):
        body = self._middleware.get_rows_from_message(body)

        logging.debug(f"GOT BATCH: {body}")

        if body[0][END_TRANSMISSION_END_INDEX] == END_TRANSMISSION_MESSAGE:
            logging.debug("Recived END transmssion")
            session_id = body[0][END_TRANSMISSION_CLIENT_ID]
            msg_id = body[0][END_TRANSMISSION_MSG_ID_INDEX]

            self.__handle_end_transmssion(session_id, msg_id)

            self._middleware.ack(delivery_tag)

            return  

        # {client_id: {
        #   Windows: [MSG_ID1, MSG_ID2, ...], 
        #   Linux: [MSG_ID1, MSG_ID2, ...], 
        #   Mac: [MSG_ID1, MSG_ID2, ...]}, 
        # ...}
        body = self.__purge_duplicates_and_add_unique_msg_id(body)

        storage.sum_batch_to_records_per_client(
            self.storage_dir,
            body,
            self._activity_log,
        )

        self._middleware.ack(delivery_tag)

    def __handle_end_transmssion(self, session_id: str, msg_id: str):
        if not os.path.exists(os.path.join(self.storage_dir, session_id)):
            logging.debug('Recived END, but client directory was already deleted. Propagating END')
            self._middleware.send_end(
                queue=self.publish_queue, 
                end_message=[session_id, msg_id, END_TRANSMISSION_MESSAGE]
            )
            return 
        
        self.__send_results(session_id, msg_id)


    def __generate_unique_msg_id(self, platform: str, msg_id: str) -> str:
        '''
        Generated msg id is just the first char of platform as ascii concatenated
        to the msg id. 
        
        This assumes that platform initial is unique for each platform.
        '''
        return str(ord(platform[0])) + msg_id

    def __purge_duplicates_and_add_unique_msg_id(self, batch: List[str]) -> List[str]:
        # CADA mensaje individual me tengo que fijar si esta duplicado, incluido dentro del mismo batch
        # (se puede optimizar en el middleware y cambiarlo aca tmb dsps)
        batch_msg_ids = set()
        filtered_batch = []
        for msg in batch:
            # Add a custom msg id based on plaftorm
            platform = msg[REGULAR_MESSAGE_FIELD_TO_COUNT_BY]
            msg[REGULAR_MESSAGE_MSG_ID] = self.__generate_unique_msg_id(platform, msg[REGULAR_MESSAGE_MSG_ID])
            
            msg_id = msg[REGULAR_MESSAGE_MSG_ID] 
            client_id = msg[REGULAR_MESSAGE_CLIENT_ID]
            if not self._activity_log.is_msg_id_already_processed(client_id, msg_id) and not msg_id in batch_msg_ids: 
                filtered_batch.append(msg)
                batch_msg_ids.add(msg_id)
            else: 
                if msg_id in batch_msg_ids:
                    logging.debug(f'[DUPLICATE FILTER] Filtered {msg_id} beacause it was repeated (inside batch)')
                else:
                    logging.debug(f'[DUPLICATE FILTER] Filtered {msg_id} beacause it was repeated (previously processed)')

        return filtered_batch
    

    def __send_results(self, session_id: str, end_msg_id: str):
        # TODO: Hacer que si no existe el directorio, simplemente propague el end 
        PLATFORM = 0
        COUNT = 1
        self._middleware.create_queue(self.publish_queue)

        client_dir = f"{self.storage_dir}/{session_id}"
        reader = storage.read_all_files(client_dir)

        for record in reader:
            logging.debug(f"sending record: {record}")
            if self._got_sigterm:
                # should send everything so i can ack before closing
                # or return false so end is not acked and i dont send the results?
                return

            self._middleware.publish(
                [session_id, record[PLATFORM], record[COUNT]], queue_name=self.publish_queue
            )

        self._middleware.publish_batch(self.publish_queue)
        self._middleware.send_end(
            queue=self.publish_queue, 
            end_message=[session_id, end_msg_id, END_TRANSMISSION_MESSAGE]
        )
        logging.debug(f"Sent results to queue {self.publish_queue}")

        # storage.delete_directory(client_dir)

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        # self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
