import csv
import os
import signal
import logging
from common.middleware.middleware import Middleware, MiddlewareError
from common.storage import storage
from common.protocol.protocol import Protocol
from common.activity_log.activity_log import ActivityLog
from typing import *

END_TRANSMISSION_MESSAGE = "END"

END_TRANSMISSION_MESSAGE_INDEX = 1
END_TRANSMISSION_SESSION_ID = 0

REGULAR_MESSAGE_SESSION_ID = 0
REGULAR_MESSAGE_ID = 1
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

        temp_file = os.path.join(dir, f"temp_{file_name}")
        with open(temp_file, mode='w', newline='') as temp:
            writer = csv.writer(temp)
            for line in file_state:
                writer.writerow(line.split(','))

        os.replace(temp_file, full_file_path)

    def __handle_message(self, delivery_tag: int, body: List[List[str]]):
        body = self._middleware.get_rows_from_message(body)

        logging.debug(f"GOT BATCH: {body}")

        if body[0][END_TRANSMISSION_MESSAGE_INDEX] == END_TRANSMISSION_MESSAGE:
            logging.debug("Recived END transmssion")
            session_id = body[0][END_TRANSMISSION_SESSION_ID]

            self.__send_results(session_id)
            self._middleware.ack(delivery_tag)

            return

        # {client_id: {
        #   Windows: [MSG_ID1, MSG_ID2, ...], 
        #   Linux: [MSG_ID1, MSG_ID2, ...], 
        #   Mac: [MSG_ID1, MSG_ID2, ...]}, 
        # ...}
        msg_ids_per_record_by_client_id = self.__group_msg_ids_per_client_by_platform(body)
        clone = msg_ids_per_record_by_client_id.copy()
        self.__purge_duplicates(msg_ids_per_record_by_client_id)
        
        if clone != msg_ids_per_record_by_client_id: 
            logging.debug(f'BEFORE: {clone}')
            logging.debug(f'AFTER: {msg_ids_per_record_by_client_id}')

        storage.sum_platform_batch_to_records_per_client(
            self.storage_dir,
            msg_ids_per_record_by_client_id,
            self._activity_log
        )

        self._middleware.ack(delivery_tag)

    def __purge_duplicates(self, msg_ids_per_record_by_client_id: Dict[str, Dict[str, List[str]]]):
        # Para cada cliente o se actualizo el archivo completo o no se actualizo, eso queire decir
        # que si al menos UNO de los msg_id ya estaba loggeado, ya se proceso totalemente ese cliente

        client_ids_to_remove = [] # No se puede remover claves de un dict mientras se itera
        for client_id, count_per_platform in msg_ids_per_record_by_client_id.items():
            for platform, msg_ids in count_per_platform.items(): # TODO: Por ahi hay alguna forma mejor de hacer esto
                an_arbitrary_message_id = msg_ids[0]
                if self._activity_log.is_msg_id_already_processed(client_id, an_arbitrary_message_id):
                    client_ids_to_remove.append(client_id)
                
                break
        
        for client_id in client_ids_to_remove:
            del msg_ids_per_record_by_client_id[client_id]


    def __group_msg_ids_per_client_by_platform(
        self, body: list[list]
    ) -> Dict[str, Dict[str, List[str]]]:
        '''
        Retorna una lista con los msg ids involucrados para cada mensaje, sacar la cantidad a sumar
        es hacer len(lista) por eso no se guarda el count tambien.

        Se devuelve algo del estilo: 
        {client_id: {Windows: ['1,W', '3,W', '4,W', '5,W'], Linux: ['2,L', '10,L'], Mac: ['9,M']}, ...}

        Explicacion message id: 
        Como un mismo mensaje obtiene si el juego esta soportado para una plataforma o no, pero el drop nulls
        manda todos los mensajes con un mismo msg_id, que esta bien, porque para el es todo un mismo mensaje,
        yo tengo que agregar un identificador unico para cada uno (porque para el count by platform son distintos 
        mensajes), para evitar guardar estado, se guarda msg_id,inicial_platform, asi se diferencian
        '''
        msg_ids_per_record_by_client_id = {}
        for record in body:
            msg_id = record[REGULAR_MESSAGE_ID]
            client_id = record[REGULAR_MESSAGE_SESSION_ID]
            record_id = record[REGULAR_MESSAGE_FIELD_TO_COUNT_BY]

            if not client_id in msg_ids_per_record_by_client_id:
                msg_ids_per_record_by_client_id[client_id] = {}

            if not record_id in msg_ids_per_record_by_client_id[client_id]:
                msg_ids_per_record_by_client_id[client_id][record_id] = []

            actual_message_id = ','.join([msg_id, record_id[0]]) # Ver explicacion del inicio
            msg_ids_per_record_by_client_id[client_id][record_id].append(actual_message_id)

        return msg_ids_per_record_by_client_id

    def __send_results(self, session_id: str):
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
                [session_id, record[0], record[1]], queue_name=self.publish_queue
            )

        self._middleware.publish_batch(self.publish_queue)
        self._middleware.send_end(
            queue=self.publish_queue, end_message=[session_id, END_TRANSMISSION_MESSAGE]
        )
        logging.debug(f"Sent results to queue {self.publish_queue}")

        storage.delete_directory(client_dir)

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")
        self._got_sigterm = True
        # self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
