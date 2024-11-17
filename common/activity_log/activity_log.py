import sys
from os import SEEK_END, makedirs, path
import os
import csv
import shutil
from pathlib import Path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from constants import *
from typing import * 
from operations import Operation, RecoveryOperation
from protocol.protocol import Protocol, ProtocolError

class ActivityLog:
    def __init__(self, protocol=Protocol(), show_corrupted=False):
        self._dir = './log'
        makedirs(self._dir, exist_ok=True)

        # Tengo un log General (que es el ultimo mensaje nada mas) y 
        # un log por cliente, que solamente se guarda el numero de mensaje
        # para loggearlo
        self._general_log_path = f'{self._dir}/{GENERAL_LOG_FILE_NAME}'
        self.__create_log_file(self._general_log_path)

        self._protocol = protocol
        self._show_corrupted_lines = show_corrupted

    def __create_client_log_file(self, client_id: str):
        '''
        Only creates it if it doesn't already exist
        ''' 
        full_client_dir_path = f'{self._dir}/{client_id}'
        makedirs(full_client_dir_path, exist_ok=True)

        full_path = f'{full_client_dir_path}/{CLIENT_LOG_FILE_NAME}'
        self.__create_log_file(full_path)


    def __create_log_file(self, full_path: str):
        if not path.exists(full_path):
            open(full_path, 'w').close()

    def __get_line_for_general_log(self, client_id: str, data: List[str], msg_ids: List[str]) -> bytes:
        row = [client_id] + data + msg_ids
        return self._protocol.encode(row, add_checksum=True)

    def _log_to_processed_lines(self,  client_id: str, msg_id: str):
        '''
        Lines are added in an arbitrary order 
        '''
        self.__create_client_log_file(client_id)

        full_path = f"{self._dir}/{client_id}/{CLIENT_LOG_FILE_NAME}"
        full_temp_file_path = f"{self._dir}/{client_id}/temp_sorted.csv"
        
        inserted_new_msg_id = False
        with open(full_path, mode="r") as infile, open(
            full_temp_file_path, mode="w", newline=""
        ) as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            for read_msg_id in reader:
                read_msg_id = read_msg_id[0]

                if not inserted_new_msg_id and msg_id < read_msg_id:
                    writer.writerow([msg_id])
                    inserted_new_msg_id = True
                
                writer.writerow([read_msg_id])

            if not inserted_new_msg_id:
                writer.writerow([msg_id])

        os.replace(full_temp_file_path, full_path) 

    def _log_to_general_log(self, client_id: str, data: List[str], msg_ids: List[str]): 
        # La linea se pisa todo el rato, pq? porque si yo estoy escribiendo 
        # denuevo una linea (llegue hasta esta funcion), eso quiere decir
        # que ya se termino de bajar a disco la linea anterior (es
        # secuencial), por lo tanto ya no necesito la linea anterior
        line = self.__get_line_for_general_log(client_id, data, msg_ids)
        with open(self._general_log_path, 'wb') as log: 
            log.write(line)

    def __log(self, client_id: str, data: List[str], msg_ids: List[str]): 
        # Si se rompe mientras se hace el log general 
        #       -> Si no se llego a loggear completo salta el checksum y no se re-hace nada
        #       -> Si se llego a loggear completo, se re-hace el bajado a disco (se guarda estado por lo tanto
        #          se puede hacer multiples veces)
        #       -> Para cada msg_id involucrado en el log, me fijo si se pudo bajar 
        #          a disco, si no se pudo bajar lo agrego, entonces no hay problema 
        #          conque se rompa mientras se guardan las lineas que se procesaron
        self.__log_to_general_log(client_id, data)    
        for msg_id in msg_ids:
            self.__log_to_processed_lines(client_id, msg_id)

    def read_general_log(self):
        '''
        Generator that returns each line of the file, the \n
        at the end of the line is not returned 
        '''
        with open(self._general_log_path, 'rb') as log:
            for line in log:
                decoded_line = self._protocol.decode(line, has_checksum=True)
                if len(decoded_line) != 0:
                    decoded_line[-1] = decoded_line[-1].strip()

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
        # o hacer binary search de alguna forma 
        full_path = f'{self._dir}/{client_id}/{CLIENT_LOG_FILE_NAME}'
        with open(full_path, 'r') as log:
            reader = csv.reader(log)

            for row in reader:
                row = row[0]
                if row == msg_id:
                    return True
                
        return False 

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

    def get_recovery_operation(self):
        for line in self.read_log_in_reverse():
            if line[0] == Operation.COMMIT.message():
                return RecoveryOperation.REDO
            else: 
                return RecoveryOperation.ABORT

    def restore(self): 
        '''
        Generator that first returns the action to perform (RecoveryOperation.REDO or RecoveryOperation.ABORT)
        and then the corresponding lines to either REDO or ABORT
        '''
        '''
        Para el ultimo commit, ya sea si se completo o no: 
        Restaurar el estado implica verificar la ultima Ti commiteo, si commiteo
        entonces hay que re-hacer todo lo que hizo, si no commiteo, entonces hay 
        que abortar, es decir, deshacer todo lo que se hizo. 

        Ambas operaciones implican saber donde esta guardada la data, hay dos opciones:
        - O lo hace el activity log
        - O el activity log dice que operacion hay que hacer, y con que data (el batch size)
        va ser acotado en general, asi que no deberia haber problema devolviendo que borrar

        Que lo haga el activity log, tambien implica que sepa como esta guardada la data, lo
        cual hace que este mas acoplado todavia a la implementacion de los nodos 
        '''
        # - Si la ultima linea leida es un commit, tengo que re-guardar todo lo de ese batch
        # - Si la ultima linea leida es un commit corrupto (tengo la palabra commit, pero no el numero de batch por eh),
        # tengo que re-guardar todo lo de ese batch
        # - Si la ultima linea leida no es un commit, tengo que abortar la tx
        for index, line in enumerate(self.read_log_in_reverse()):
            if index == 0:
                if line[0] != Operation.COMMIT.message():
                    # TODO: Verificar de alguna forma si esta corrupta? 
                    # Si estaba corrupta la linea y no rompio por alguna razon
                    # el parseo va a intentar re-hacer algo que estaba mal el nodo
                    # y es un problema 
                    yield line[1:]

                continue
            
            if line[0] == Operation.BEGIN.message(): 
                break

            yield line[1:] 

