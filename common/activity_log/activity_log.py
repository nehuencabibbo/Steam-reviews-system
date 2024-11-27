import sys
import os
import shutil
import logging 
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import * 
from storage.storage import (
    atomically_append_to_file,
    create_file_if_unexistent,
)
from .constants import * 


class ActivityLogError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message

class ActivityLog:
    def __init__(self, log_two_ends: bool = False, range_for_partition: int = 20):
        self._dir = './log'
        os.makedirs(self._dir, exist_ok=True)

        self._log_two_ends = log_two_ends 
        # Cantidad de ends a loggear, UNICAMENTE para el join, es 2
        # porque loggea los ends de juegos y reviews
        self._range_for_partition = range_for_partition
        self._procesed_lines_file_prefix = 'procesed_lines'
        self._ends_file_name = 'ends.txt'
        self._middleware_dir = os.path.join(self._dir, 'middleware')
        # Range used to separte the distinct processed lines onto files

        # Tengo un log General (que es el ultimo mensaje nada mas) y 
        # un log por cliente, que solamente se guarda el numero de mensaje
        # para loggearlo
        self._general_log_path = f'{self._dir}/{GENERAL_LOG_FILE_NAME}'
        create_file_if_unexistent(self._general_log_path)

    '''
    UTILITY
    '''
    def __add_checksum(self, line: bytes) -> bytes: 
        length = len(line).to_bytes(CHECKSUM_LENGTH_BYTES, byteorder='big', signed=False)

        return length + line
    
    def _get_partition_file_name(self, msg_id: int):
        return f"{self._procesed_lines_file_prefix}_{msg_id//self._range_for_partition}.txt"
    
    def _get_ends_file_path(self, client_id: str): 
        return os.path.join(self._dir, client_id, self._ends_file_name)
    
    def __add_end_to_client(self, client_id: str, end_logging: int = 0):
        temp_file_path = os.path.join(self._dir, client_id, 'temp.txt')
        file_path = self._get_ends_file_path(client_id)

        with open(file_path, 'r') as original, open(temp_file_path, 'w') as temp: 
            amount = original.readline().strip()
            # File was empty
            if amount == '':
                line_to_write = '1'
                if self._log_two_ends:
                    line_to_write = ['0', '0']
                    line_to_write[end_logging] = '1'
                    line_to_write = ','.join(line_to_write)

                temp.write(line_to_write + '\n')
            else: 
                if self._log_two_ends:
                    # Solo le sumo a la linea correspondiente
                    line_to_write = amount.split(',')
                    line_to_write[end_logging] = str(int(line_to_write[end_logging]) + 1)
                    line_to_write = ','.join(line_to_write)

                    temp.write(line_to_write + '\n')
                else: 
                    line_to_write = f'{int(amount) + 1}'

                    temp.write(line_to_write + '\n')

        os.replace(temp_file_path, file_path)

    def __append_msg_ids(self, dir: str, file_name: str, msg_ids: List[str]):
        # If file is large this is really ineficient
        temp_file_path = os.path.join(dir, 'temp_append.txt')
        file_path = os.path.join(dir, file_name)

        create_file_if_unexistent(file_path)
        found_duplicate = False
        with open(file_path, 'r') as original, open(temp_file_path, 'w') as temp: 
            for line in original: 
                line = line.strip()
                for msg_id in msg_ids:
                    if line == msg_id:
                        found_duplicate = True
                        logging.debug(f'Found duplicate, discarding: {msg_ids}')
                        break
                if found_duplicate: break
                temp.write(line + '\n')

            for msg_id in msg_ids:
                temp.write(msg_id + '\n')

        if found_duplicate:
            os.remove(temp_file_path)
        else:
            os.replace(temp_file_path, file_path)

    def _get_file_name_for_middleware_queue(self, queue_name: str) -> str: 
        return f'{queue_name}.bin'
    
    def __override_end_file_value(self, client_id: str, value: int):
        os.makedirs(os.path.join(self._dir, client_id), exist_ok=True)

        temp_file_path = os.path.join(self._dir, client_id, 'temp.txt')
        file_path = self._get_ends_file_path(client_id)

        with open(temp_file_path, 'w') as temp: 
            temp.write(f'{value}\n')

        os.replace(temp_file_path, file_path)
    
    '''
    GENREAL LOG 
    ''' 
    # TODO: Moverlo a una clase y que lo reciba por parametro asi puedo mockear y 
    # testear corruptos
    def __get_line_for_general_log(self, msg: List[str], client_id: str = None, log_type: str = None) -> bytes:
        if client_id: msg = [client_id] + msg
        if log_type: msg = [log_type] + msg # if there's a log type then it goes first [log_type, client_id, rest...]
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
    def __get_msg_ids_by_file(self, msg_ids: List[str]) -> dict[str, list[str]]:
        msg_ids_by_file = {}
        for msg_id in msg_ids:
            try:
                file_name = self._get_partition_file_name(int(msg_id))
                msg_ids_by_file[file_name] = msg_ids_by_file.get(file_name, [])
                msg_ids_by_file[file_name].append(msg_id)
            except ValueError as e:
                logging.error(f"Received {msg_id}, Expected a numerical type in its place")
                raise e

        return msg_ids_by_file


    def _log_to_processed_lines(self, client_id: str, msg_ids: List[str]): 
        msg_ids_by_file = self.__get_msg_ids_by_file(msg_ids)
        # {
        #   'procesed_lines_75290.csv': ['1', '2', '3', ...], 
        #   'procesed_lines_118045.csv': ['50', '40', '30'],
        # }
        client_dir = os.path.join(self._dir, client_id)
        os.makedirs(client_dir, exist_ok=True)
        for file_name, msg_ids in msg_ids_by_file.items():
            self.__append_msg_ids(client_dir, file_name, msg_ids)


    def _log_to_general_log(self, client_id: str, data: List[str], msg_ids: List[str], log_type: str): 
        # La linea se pisa todo el rato, pq? porque si yo estoy escribiendo 
        # denuevo una linea (llegue hasta esta funcion), eso quiere decir
        # que ya se termino de bajar a disco la linea anterior (es
        # secuencial), por lo tanto ya no necesito la linea anterior
        data_in_bytes = self.__get_line_for_general_log(data, log_type=log_type)
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
        self._log_to_general_log(client_id, data, msg_ids, GENERAL_LOGGING)
        self._log_to_processed_lines(client_id, msg_ids)

    def log_end(self, client_id: str, msg_id: str, end_logging: int = 0):
        '''
        If log_two_ends = True on startup, then end_logging should be specified.

        log_two_ends indicates that amount_of_ends1,amount_of_ends2 will be logged to
        end log

        end_logging = 0 will add 1 to amount_of_ends1, and end_logging = 1 will update 
        amount_of_ends2
        '''
        # Hay un par de casos aca: 
        #       1 - Se cae antes de loggear al log general => No pasa nada
        #       2 - Se cae mientras loggea al log general => No pasa nada
        #       3 - Se cae justo despues de loggear al log general => 
        #               Se recupera el estado tanto de los ENDs como
        #               como de los mensajes procesados
        #       4 - Se cae mientras le agrega al end => Es atomica la operacion,
        #               si se produce esta todo bien, si no se produce al recuperar
        #               se va a leer el log general y se va a recuperar el estado
        #       5 - Se cae justo despues de agregar al END => El msg id se va a 
        #           recuperar con el log general, y al recuperar el estado del end
        #           al ser el mismo que el actual se va a pisar y no va a pasar nada
        #       6 - Se cae en el medio de loggear las lineas procesadas => No hay problema, 
        #           se recuperan con el log general
        msg_id = [msg_id]
        new_amount_of_ends = self._get_amount_of_ends(client_id)
        # Si se estan loggeando dos ends (end1,end2), solo le sumo al que me indican por parametro
        if self._log_two_ends:
            new_amount_of_ends = new_amount_of_ends.split(',')
            new_amount_of_ends[end_logging] = str(int(new_amount_of_ends[end_logging]) + 1)
            new_amount_of_ends = ','.join(new_amount_of_ends)
        else: 
            new_amount_of_ends = str(int(new_amount_of_ends) + 1)
        data_to_log = [
            self._get_ends_file_path(client_id),
            new_amount_of_ends,
        ]
        self._log_to_general_log(client_id, data_to_log, msg_id, END_LOGGING)
        self.__add_end_to_client(client_id, end_logging=end_logging)
        self._log_to_processed_lines(client_id, msg_id)

    def log_for_middleware(self, queue_name: str, msg: bytes):
        # En caso de querer agregar tambien las lineas procesadas para filtrar duplicados
        # es simplemente agregar log_to_processed_lines y clavarle de client_id middleware,
        # todo se va a ir a una carpeta middleware/processed_lines
        file_name = self._get_file_name_for_middleware_queue(queue_name)
        full_path = os.path.join(self._middleware_dir, file_name)

        os.makedirs(self._middleware_dir, exist_ok=True)
        create_file_if_unexistent(full_path)

        msg_with_checksum = self.__add_checksum(msg)
        with open(full_path, 'ab') as log:
            log.write(msg_with_checksum)

    '''
    READING LOG
    '''
    def _get_amount_of_ends(self, client_id: str) -> str: 
        '''
        Returns the raw amount of ends read without \n, 
        if client had 0 ends, then '0' is returned.
        
        if log_two_ends is set to True and client has
        no ends, then 0,0 is returned
        '''
        full_path = self._get_ends_file_path(client_id)
        os.makedirs(os.path.join(self._dir, client_id), exist_ok=True) 
        create_file_if_unexistent(full_path)

        with open(full_path, 'r') as ends:
            # File is only one line
            amount = ends.readline().strip()

        if self._log_two_ends:
            return '0,0' if amount == '' else amount
        
        return '0' if amount == '' else amount

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
    # TODO: Por ahi se puede recibir un grupo de msg_ids, agruparlos por archivo y buscar 
    # de a grupos para no abrir y cerrar un archivo tantas veces
    def is_msg_id_already_processed(self, client_id: str, msg_id: str) -> bool:
        full_path = os.path.join(
            self._dir,
            client_id, 
            self._get_partition_file_name(int(msg_id))
        )

        if not os.path.exists(full_path):
            return False 
        
        with open(full_path, 'r') as log:
            for line in log:
                line = line.strip()
                if line == msg_id:
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
        log_type = None
        read_lines = 0
        try:
            for line_type, line in enumerate(self.read_general_log()):
                if line_type == FILE_STATE_LINE: 
                    log_type = line[0]
                    full_file_path_to_recover = line[1]
                    file_state = line[2:]

                elif line_type == MSG_IDS_LINE: 
                    client_id = line[0]
                    msg_ids =  line[1:]
                    if log_type == END_LOGGING:
                        # Si se loggeo un END, entonces recupero el estado del archivo
                        # directamente aca porque todo lo de los ends es interno
                        self.__override_end_file_value(client_id, int(file_state[0]))

                    self._log_to_processed_lines(client_id, msg_ids)
                
                read_lines += 1

        except ActivityLogError as _:
            return None, None 
        
        if read_lines != 2: 
            # Es re dificil que pase, pero puede llegar a leer solo una bien
            return None, None 
        
        if log_type == END_LOGGING:
            # Si se estaba loggeando un end toda la recuperacion es interna
            return None, None

        return full_file_path_to_recover, file_state
    

    def recover_middleware_state(self) -> Dict[str, Tuple[bytes, int]]: 
        '''
        Returns the original middleware state, as a dictionary
        where the key is the corresponding queue_name and the 
        value is a tuple (batch, amount_of_msgs)
        '''
        result = {}
        for f in os.listdir(self._middleware_dir): 
            # No hace falta checkear si son archivos o dir pq solo
            # se guardan archivos aca
            queue_name = f.rsplit('.', maxsplit=1)[0]
            full_path = os.path.join(self._middleware_dir, f)
            with open(full_path, 'rb') as log:
                data = log.read()

            while len(data) > 0: 
                checksum = int.from_bytes(data[:CHECKSUM_LENGTH_BYTES], "big", signed=False)
                data = data[CHECKSUM_LENGTH_BYTES:]

                if checksum < CHECKSUM_LENGTH_BYTES:
                    # Solo la ultima linea puede estar corrupta, 
                    # si llega a estarlo, implica que ni se guardo
                    # en el middleware porque primero se loggea y despues
                    # se guarda, la descarto
                    continue

                msg = data[:checksum]
                data = data[checksum:]

                if len(msg) < checksum:
                    # idem checkeo anterior
                    continue
                
                batch, count = result.get(queue_name, (b'', 0))
                result[queue_name] = (batch + msg, count + 1)

        return result
    
    def recover_ends_state(self):
        '''
        Returns the original ENDS state, for reach client, it returns
        the amount of recived ends.

        If log_two_ends = True, then two dictioiaries, one for each end are
        returns, any other case just one is returned
        '''
        result = {}
        if self._log_two_ends:
            result2 = {}
        for client_id in os.listdir(self._dir): 
            if not os.path.isdir(os.path.join(self._dir, client_id)):
                continue
            
            amount = self._get_amount_of_ends(client_id)
            if self._log_two_ends:
                amount = amount.split(',')

                result[client_id] = int(amount[0])
                result2[client_id] = int(amount[1])
            else: 
                result[client_id] = int(amount)

        if self._log_two_ends:
            return result, result2
        
        return result