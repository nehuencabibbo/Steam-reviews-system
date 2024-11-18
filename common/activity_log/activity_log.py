import sys
from os import SEEK_END, makedirs, path
import os
import csv
import shutil
import logging 
from pathlib import Path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from typing import * 
# from operations import Operation, RecoveryOperation
from protocol.protocol import Protocol, ProtocolError
from storage.storage import create_file_if_unexistent
from .constants import * 

# GENERAL_LOG_FILE_NAME="general_log.bin"
# CLIENT_LOG_FILE_NAME="client_log.csv"

FIELD_LENGTH_BYTES = 4
CHECKSUM_LENGTH_BYTES = 4

class ActivityLogError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message

class ActivityLog:
    def __init__(self, show_corrupted=False):
        self._dir = './log'
        makedirs(self._dir, exist_ok=True)

        # Tengo un log General (que es el ultimo mensaje nada mas) y 
        # un log por cliente, que solamente se guarda el numero de mensaje
        # para loggearlo
        self._general_log_path = f'{self._dir}/{GENERAL_LOG_FILE_NAME}'
        create_file_if_unexistent(self._general_log_path)

        self._show_corrupted_lines = show_corrupted

    '''
    UTILITY
    '''
    def __create_client_log_file(self, client_id: str):
        '''
        Only creates it if it doesn't already exist
        ''' 
        full_client_dir_path = f'{self._dir}/{client_id}'
        makedirs(full_client_dir_path, exist_ok=True)

        full_path = f'{full_client_dir_path}/{CLIENT_LOG_FILE_NAME}'
        create_file_if_unexistent(full_path)

    def __add_checksum(self, line: bytes) -> bytes: 
        length = len(line).to_bytes(4, byteorder='big', signed=False)

        return length + line
    '''
    GENREAL LOG 
    ''' 
    # TODO: Moverlo a una clase y que lo reciba por parametro asi puedo mockear y 
    # testear corruptos
    def __get_line_for_general_log(self, msg: List[str], client_id: str = None) -> bytes:
        if client_id: msg = [client_id] + msg
        return self.__encode_for_general_log(msg)
    
    def __encode_for_general_log(self, line: List[str]) -> bytes:
        result = b""
        for field in line:
            encoded_field = field.encode("utf-8")
            encoded_field_length = len(encoded_field).to_bytes(
                FIELD_LENGTH_BYTES, "big", signed=False
            )
            result += encoded_field_length
            result += encoded_field

        result = self.__add_checksum(result)

        return result

    def __decode_general_log_line(self, line: bytes) -> Tuple[List[str], int]:
        '''
        Returns a decoded line and the amount of bytes read.
        Validates checksum
        '''
        checksum_bytes = line[:CHECKSUM_LENGTH_BYTES]
        if len(checksum_bytes) < CHECKSUM_LENGTH_BYTES: 
            raise ActivityLogError((
                f'Invalid checksum length, should be {CHECKSUM_LENGTH_BYTES}, '
                f'but was {len(checksum_bytes)}'
            ))
        
        checksum = int.from_bytes(checksum_bytes, byteorder='big')
        line = line[CHECKSUM_LENGTH_BYTES:] # Saco los bytes del checksum

        line = line[:checksum] # Me quedo solo con la linea a leer 
        if len(line) < checksum:
            raise ActivityLogError('Checksum does not match')
        
        result = []
        while len(line) > 0:
            field_length = int.from_bytes(line[:FIELD_LENGTH_BYTES], "big", signed=False)
            line = line[FIELD_LENGTH_BYTES:]

            field = line[:field_length]
            field = field.decode("utf-8")
            result.append(field)
            line = line[field_length:]

        return (result, CHECKSUM_LENGTH_BYTES + checksum)


    '''
    GENERAL LOGGING
    '''
    # TODO: Mover a una clase
    def _log_to_processed_lines(self, client_id: str, msg_id: str):
        '''
        Lines are added in ascending order. If line is already present, then it's skipped
        as msg ids for a determined client should be unique
        '''
        self.__create_client_log_file(client_id)

        full_path = f"{self._dir}/{client_id}/{CLIENT_LOG_FILE_NAME}"
        full_temp_file_path = f"{self._dir}/{client_id}/temp_sorted.csv"
        
        duplicated_msg_id = False 
        inserted_new_msg_id = False
        with open(full_path, mode="r") as infile, open(
            full_temp_file_path, mode="w", newline=""
        ) as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            for read_msg_id in reader:
                read_msg_id = read_msg_id[0]

                if not inserted_new_msg_id:
                    if msg_id < read_msg_id:
                        writer.writerow([msg_id])
                        inserted_new_msg_id = True
                    elif msg_id == read_msg_id:
                        # No habia que updatear nada en el archivo, no puede
                        # haber duplicados
                        duplicated_msg_id = True 
                        break

                writer.writerow([read_msg_id])

            if not inserted_new_msg_id and not duplicated_msg_id:
                writer.writerow([msg_id])

        if not duplicated_msg_id:
            os.replace(full_temp_file_path, full_path) 
        else: 
            # si no se remueve no hay problema pq despues se vuelve a pisar 
            os.remove(full_temp_file_path)

    def _log_to_general_log(self, client_id: str, data: List[str], msg_ids: List[str]): 
        # La linea se pisa todo el rato, pq? porque si yo estoy escribiendo 
        # denuevo una linea (llegue hasta esta funcion), eso quiere decir
        # que ya se termino de bajar a disco la linea anterior (es
        # secuencial), por lo tanto ya no necesito la linea anterior
        data_in_bytes = self.__get_line_for_general_log(data)
        msg_ids_in_bytes = self.__get_line_for_general_log(msg_ids, client_id=client_id)
        # logging.debug(f'EXPECTED DATA: {data_in_bytes}')
        # logging.debug(f'EXPECTED MSG_IDS: {msg_ids_in_bytes}')
        with open(self._general_log_path, 'wb') as log: 
            log.write(data_in_bytes)
            log.write(msg_ids_in_bytes)
            log.flush()
            os.fsync(log.fileno())

    def log(self, client_id: str, data: List[str], msg_ids: List[str]): 
        # Si se rompe mientras se hace el log general 
        #       -> Si no se llego a loggear completo salta el checksum y no se re-hace nada
        #       -> Si se llego a loggear completo, se re-hace el bajado a disco (se guarda estado por lo tanto
        #          se puede hacer multiples veces)
        #       -> Para cada msg_id involucrado en el log, me fijo si se pudo bajar 
        #          a disco, si no se pudo bajar lo agrego, entonces no hay problema 
        #          conque se rompa mientras se guardan las lineas que se procesaron

        # Para que el general log sea valido tiene que tener dos lineas y ambas tienen que estar
        # integras 
        self._log_to_general_log(client_id, data, msg_ids)
        for msg_id in msg_ids:
            self._log_to_processed_lines(client_id, msg_id)

    '''
    READING LOG
    '''
    def read_general_log(self):
        '''
        Generator that returns each line of the file, the \n
        at the end of the line is not returned 
        '''
        with open(self._general_log_path, 'rb', buffering=0) as log:
            data = log.read()
        
        while len(data) > 0:
            line, bytes_read = self.__decode_general_log_line(data)
        
            data = data[bytes_read:]

            yield line


    def read_processed_lines_log(self, client_id: str):
        '''
        Generator that returns each line of the file, the \n
        at the end of the line is not returned 
        '''
        full_path = f'{self._dir}/{client_id}/{CLIENT_LOG_FILE_NAME}'
        with open(full_path) as log:
            reader = csv.reader(log)
            for row in reader:
                yield row[0]

    '''
    CLEAN UP
    '''
    # TODO: Llamar a estas funciones cuando llega el sigterm y purgear los temp
    # files el arrancar
    def remove_client_logs(self, client_id: str):
        '''
        Removes client log folder and it's contents
        '''
        client_folder_full_path = f'{self._dir}/{client_id}'
        if Path(client_folder_full_path).exists():
            shutil.rmtree(client_folder_full_path)

    def remove_all_logs(self):
        '''
        Removes every client log, the general log
        and the log folder
        '''
        if Path(self._dir).exists():
            shutil.rmtree(self._dir)

    '''
    DUPLICATE FILTER
    '''
    def is_msg_id_already_processed(self, client_id: str, msg_id: str):
        # TODO: Cambiar por algo mas eficiente, particionar al guardar
        # o hacer binary search de alguna forma (no se puede sin hacer
        # demasiadas syscalls al pedo)
        full_path = f'{self._dir}/{client_id}/{CLIENT_LOG_FILE_NAME}'
        if not os.path.exists(full_path):
            return False 
        
        with open(full_path) as log:
            reader = csv.reader(log)
            for row in reader:
                row = row[0]
                if row == msg_id:
                    return True
                
        return False 
    
    '''
    RECOVERY
    '''
    def recover(self) -> Tuple[Optional[str], Optional[List[str]]]: 
        '''
        Returns state to recover based on the general log.
        File state and name is returned as what to do with it is dependant on the user.
        Processed messages state is automatically recovered by this function.
        '''
        # EL log general es el que sirve para recuperacion, tiene dos lineas nada mas,
        # el ultimo estado updateado de un determinado archivo, y los msg ids que involucraron
        # ese cambio de estado.
        # Aca hay varios casos en donde se puede romeper:
        #       - 1: Se rompe sin loggear ni la primera linea
        #       - 2: Se rompe en a mitad de la linea del log 
        #       - 3: Se rompe justo despues de terminar de escribir la primera linea del log
        #       - 4: Se rompe durante la linea de los msg ids
        #       - 5: Se rompe justo despues de la linea de los msg_ids
        #
        # Soluciones:
        #       - 1, 2, 3, 4: No se recupera nada, ya sea porque el estado nuevo es
        #           invalido o porque no puedo saber que mensajes produjeron ese
        #           estado, lo cual traeria problemas a la hora de detectar duplicados
        #       - 5: Hay que updatear el archivo indicado al estado que corresponde, porque
        #           no sabemos si el os.replace se hizo por completo o se aborto. 
        #           Tambien hay que recuperar los msg_ids, ya que primero se loggea a este log y luego
        #           se bajan a disco los msg_ids (ver log(...)), por lo tanto se pudo haber 
        #           loggeado solo parte de los msg_ids 
        FILE_STATE_LINE = 0
        MSG_IDS_LINE = 1
        full_file_path_to_recover = None 
        file_state = None 
        client_id = None 
        msg_ids = None 
        read_lines = 0
        try:
            for line_type, line in enumerate(self.read_general_log()):
                logging.debug(f'GENERAL LOG LINE: {line}')
                read_lines += 1
                if line_type == FILE_STATE_LINE: 
                    full_file_path_to_recover = line[0]
                    file_state = line[1:]
                elif line_type == MSG_IDS_LINE: 
                    client_id = line[0]
                    msg_ids =  line[1:]
                    for msg_id in msg_ids: 
                        self._log_to_processed_lines(client_id, msg_id)

        except ProtocolError as _:
            return None, None 
        
        if read_lines != 2: 
            # Es re dificil que pase, pero puede llegar a leer solo una bien
            return None, None 

        return full_file_path_to_recover, file_state