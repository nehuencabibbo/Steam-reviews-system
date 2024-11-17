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

GENERAL_LOG_FILE_NAME="general_log.txt"
CLIENT_LOG_FILE_NAME="client_log.csv"

class ActivityLog:
    def __init__(self, protocol=Protocol(), show_corrupted=False):
        self._dir = './log'
        makedirs(self._dir, exist_ok=True)

        # Tengo un log General (que es el ultimo mensaje nada mas) y 
        # un log por cliente, que solamente se guarda el numero de mensaje
        # para loggearlo
        self._general_log_path = f'{self._dir}/{GENERAL_LOG_FILE_NAME}'
        create_file_if_unexistent(self._general_log_path)

        self._protocol = protocol
        self._show_corrupted_lines = show_corrupted

    def __create_client_log_file(self, client_id: str):
        '''
        Only creates it if it doesn't already exist
        ''' 
        full_client_dir_path = f'{self._dir}/{client_id}'
        makedirs(full_client_dir_path, exist_ok=True)

        full_path = f'{full_client_dir_path}/{CLIENT_LOG_FILE_NAME}'
        create_file_if_unexistent(full_path)


    def __get_line_for_general_log(self, msg: List[str], client_id: str = None) -> bytes:
        if client_id: msg = [client_id] + msg
        return self._protocol.encode(msg, add_checksum=True)

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
        data = self.__get_line_for_general_log(data) + b'\n' # Se trimea al leer, no afecta al checksum
        msg_ids = self.__get_line_for_general_log(msg_ids, client_id=client_id) + b'\n'
        with open(self._general_log_path, 'wb') as log: 
            log.write(data)
            log.write(msg_ids)


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

    def read_general_log(self):
        '''
        Generator that returns each line of the file, the \n
        at the end of the line is not returned 
        '''
        with open(self._general_log_path, 'rb') as log:
            for line in log:
                line = line.strip()
                decoded_line = self._protocol.decode(line, has_checksum=True)

                yield decoded_line

    def read_processed_lines_log(self, client_id: str):
        '''
        Generator that returns each line of the file, the \n
        at the end of the line is not returned 
        '''
        full_path = f'{self._dir}/{client_id}/{CLIENT_LOG_FILE_NAME}'
        with open(full_path) as log:
            reader = csv.reader(log)
            for row in reader:
                yield row

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
        try:
            for line_type, line in enumerate(self.read_general_log()):
                if line_type == FILE_STATE_LINE: 
                    full_file_path_to_recover = line[0]
                    file_state = line[1:]
                elif line_type == MSG_IDS_LINE: 
                    client_id, msg_ids = line[0], line[1:]
                    for msg_id in msg_ids: 
                        self._log_to_processed_lines(client_id, msg_id)

        except ProtocolError as _:
            logging.debug('General log was corrupted, will not update state')

        return full_file_path_to_recover, file_state

    # https://stackoverflow.com/questions/2301789/how-to-read-a-file-in-reverse-order
    def read_general_log_in_reverse(self, full_path: str, buf_size=8192):
        """
        A generator that returns the lines of a file in reverse order.
        Supports UTF-8 encoding
        """
        with open(full_path, 'rb') as fh:
            segment = None
            offset = 0
            fh.seek(0, SEEK_END)
            file_size = remaining_size = fh.tell()
            while remaining_size > 0:
                offset = min(file_size, offset + buf_size)
                fh.seek(file_size - offset)
                buffer = fh.read(min(remaining_size, buf_size))
                # remove file's last "\n" if it exists, only for the first buffer
                if remaining_size == file_size and buffer[-1] == ord('\n'):
                    buffer = buffer[:-1]
                remaining_size -= buf_size
                lines = buffer.split('\n'.encode())
                # append last chunk's segment to this chunk's last line
                if segment is not None:
                    lines[-1] += segment
                segment = lines[0]
                lines = lines[1:]
                # yield lines in this chunk except the segment
                for line in reversed(lines):
                    # only decode on a parsed line, to avoid utf-8 decode error
                    # print(line)
                    try: 
                        yield self._protocol.decode(line, has_checksum=True)
                    except ProtocolError as e: 
                        if self._show_corrupted_lines:
                            print(f"[ACTIVITY LOG] {str(e)}")
            # Don't yield None if the file was empty
            if segment is not None:
                # print(segment)
                try:
                    yield self._protocol.decode(segment, has_checksum=True)
                except ProtocolError as e: 
                    if self._show_corrupted_lines:
                        print(f"[ACTIVITY LOG] {str(e)}")