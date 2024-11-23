import signal
import logging
from typing import *

from common.activity_log.activity_log import ActivityLog
from common.middleware.middleware import Middleware, MiddlewareError
from common.protocol.protocol import Protocol
from common.storage import storage
from utils.utils import node_id_to_send_to, group_msg_ids_per_client_by_field

END_TRANSMISSION_MESSAGE = "END"

END_MESSAGE_CLIENT_ID = 0
END_MESSAGE_END = 1

REGULAR_MESSAGE_CLIENT_ID = 0
REGULAR_MESSAGE_MSG_ID = 1
REGULAR_MESSAGE_APP_ID = 2


class CounterByAppId:

    def __init__(self, config, middleware: Middleware, activity_log: ActivityLog):
        self._config = config
        self._middleware = middleware
        self._got_sigterm = False
        self._ends_received_per_client = {}

        # Configuration attributes
        self._node_id = config["NODE_ID"]
        self._consume_queue_suffix = config["CONSUME_QUEUE_SUFIX"]
        self._amount_of_forwarding_queues = config["AMOUNT_OF_FORWARDING_QUEUES"]
        self._publish_queue = config["PUBLISH_QUEUE"]
        self._storage_dir = config["STORAGE_DIR"]
        self._range_for_partition = config["RANGE_FOR_PARTITION"]
        self._needed_ends = config["NEEDED_ENDS"]
        self._activity_log = activity_log

        signal.signal(signal.SIGTERM, self.__sigterm_handler)

        self.__recover_state()

    def run(self):
        # Creating receiving queue
        consume_queue_name = f"{self._node_id}_{self._consume_queue_suffix}"
        self._middleware.create_queue(consume_queue_name)

        # Creating forwarding queues
        self.__create_all_forwarding_queues()

        callback = self._middleware.__class__.generate_callback(self.__handle_message)
        self._middleware.attach_callback(consume_queue_name, callback)

        try:
            logging.debug("Starting to consume...")
            self._middleware.start_consuming()
        except MiddlewareError as e:
            if not self._got_sigterm:
                logging.error(e)
        finally:
            self._middleware.shutdown()

        logging.debug("Finished")

    def __create_all_forwarding_queues(self):
        for i in range(self._amount_of_forwarding_queues):
            self._middleware.create_queue(f"{i}_{self._publish_queue}")

    def __handle_message(self, delivery_tag: int, body: List[List[str]]):
        body = self._middleware.get_rows_from_message(body)

        logging.debug(f"GOT MSG: {body}")

        if body[0][END_MESSAGE_END] == END_TRANSMISSION_MESSAGE:
            client_id = body[0][END_MESSAGE_CLIENT_ID]
            self._ends_received_per_client[client_id] = (
                self._ends_received_per_client.get(client_id, 0) + 1
            )

            logging.debug(
                f"Amount of ends received up to now: {self._ends_received_per_client[client_id]} | Expecting: {self._needed_ends}"
            )
            if self._ends_received_per_client[client_id] == self._needed_ends:
                self.__send_results(client_id)

            self._middleware.ack(delivery_tag)
            return

        count_per_client_by_app_id = group_msg_ids_per_client_by_field(
            body, 
            REGULAR_MESSAGE_CLIENT_ID, 
            REGULAR_MESSAGE_MSG_ID,
            REGULAR_MESSAGE_APP_ID,
        )
        self.__purge_duplicates(count_per_client_by_app_id)

        storage.sum_batch_to_records_per_client(
            self._storage_dir,  
            count_per_client_by_app_id,
            self._activity_log,
            range_for_partition=self._range_for_partition,
        )

        self._middleware.ack(delivery_tag)

    def __recover_state(self):
        pass 

    def __purge_duplicates(self, msg_ids_per_record_by_client_id: Dict[str, Dict[str, List[str]]]):
        # Para entender, ver sum_batch_to_records_per_client de common/storage.py
        #
        # Para cada cliente, se guarda pone en un determinado rango de archivos particionados
        # una cierto count de un determinado app_id, eso quiere decir que la operacion
        # que es atomica es guardar para cada archivo de particion que se haga, como esto depende
        # mas del guardado que otra cosa, no puedo directamente aca hacer un group_by_file_dict()
        # y si para un archivo veo un duplicado, volarlo, porque quedaria acoplado raro al storage
        # el counter.
        #  
        # Aun asi es una optimizacion, porque si hay mucho de un mismo count_by_app_id (logico, los juegos
        # top le ganan por mucho a los mas chicos), entonces si en un batch vienen varios de un mismo app_id,
        # los estoy descartando a la vez
        #
        # Tambien dado que las particiones son chicas, no se cuanto optimize borrar por archivo de particion
        app_ids_to_remove_from_clients: List[Tuple[str, str]] = []
        for client_id, count_per_app_id in msg_ids_per_record_by_client_id.items():
            for app_id, msg_ids in count_per_app_id.items(): # TODO: Por ahi hay alguna forma mejor de hacer esto
                an_arbitrary_message_id = msg_ids[0]
                if self._activity_log.is_msg_id_already_processed(client_id, an_arbitrary_message_id):
                    app_ids_to_remove_from_clients.append((client_id, app_id))
                
                break

        for client_id, app_id in app_ids_to_remove_from_clients: 
            del msg_ids_per_record_by_client_id[client_id][app_id]


    def __send_results(self, client_id: str):
        queue_name = self._publish_queue

        storage_dir = f"{self._storage_dir}/{client_id}"
        reader = storage.read_all_files(storage_dir)

        for record in reader:
            logging.debug(f"RECORD READ: {record}")
            record.insert(0, client_id)
            self.__send_record_to_forwarding_queues(record)

        self.__send_last_batch_to_forwarding_queues()
        self.__send_end_to_forwarding_queues(
            prefix_queue_name=queue_name, client_id=client_id
        )

        self._clear_client_data(client_id, storage_dir)

    def __send_record_to_forwarding_queues(self, record: List[str]):
        for queue_number in range(self._amount_of_forwarding_queues):
            full_queue_name = f"{queue_number}_{self._publish_queue}"

            logging.debug(f"Sending record: {record} to queue: {full_queue_name}")

            self._middleware.publish(record, full_queue_name)

    def __send_last_batch_to_forwarding_queues(self):
        for queue_number in range(self._amount_of_forwarding_queues):
            full_queue_name = f"{queue_number}_{self._publish_queue}"

            logging.debug(f"Sending last batch to queue: {full_queue_name}")

            self._middleware.publish_batch(full_queue_name)

    def __send_end_to_forwarding_queues(self, prefix_queue_name, client_id):
        for i in range(self._amount_of_forwarding_queues):
            self._middleware.send_end(
                f"{i}_{prefix_queue_name}",
                end_message=[client_id, END_TRANSMISSION_MESSAGE],
            )
            logging.debug(f"Sent END of client: {client_id}")

    def _clear_client_data(self, client_id, storage_dir):
        if not storage.delete_directory(storage_dir):
            logging.debug(f"Couldn't delete directory: {storage_dir}")
        else:
            logging.debug(f"Deleted directory: {storage_dir}")
        self._ends_received_per_client.pop(client_id)

    def __sigterm_handler(self, signal, frame):
        logging.debug("Got SIGTERM")

        self._got_sigterm = True
        #self._middleware.stop_consuming_gracefully()
        self._middleware.shutdown()
