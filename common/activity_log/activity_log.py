from typing import * 
from operations import Operation, RecoveryOperation
from os import SEEK_END, makedirs

# TODO: Volar el BEGIN 
class ActivityLog:
    '''
    REDO de BDD: 
    - Ti modifica el item X reemplazando un valor vold
    por v, se escibe (WRITE; Ti; X; vnew) en el log
    - Ti hace commit, se escribe (COMMIT, Ti) en el log
    y se hace flush del log a disco. Luego de esto se
    escribe el valor del dato a disco
    - Al reiniciar se abortan las transacciones no 
    commiteadas
    - Aca, como esto es secuencial (se procesa un batch,
    luego otro y asi), no hacen falta checkpoints.
    - No hace falta re-hacer todo lo que commiteo porque aca
    no hay dependencias, es como si el solapamiento fuera secuencial
    de multiples transacciones Ti (que son procesar el batch i)
    '''
    def __init__(self, output_file_name: str):
        self._output_file_name = output_file_name + "_log.txt"
        self._dir = './log'
        makedirs(self._dir, exist_ok=True)

        self._full_log_path = f'{self._dir}/{self._output_file_name}'
        self.__create_log_file()

    def __create_log_file(self):
        open(self._full_log_path, 'w').close()

    def log_begin(self, batch_number: str):
        self.__log(Operation.BEGIN, batch_number)

    def log_write(self, batch_number: str, data: List[str]):
        self.__log(Operation.WRITE, batch_number, data)

    def log_commit(self, batch_number: str):
        self.__log(Operation.COMMIT, batch_number)

    def __log(self, operation: Operation, batch_number: str, data: List[str] = None): 
        line = [operation.message(), batch_number]
        if data: line += data

        joined_line = ','.join(line) + '\n'

        with open(self._full_log_path, 'a') as log:
            log.write(joined_line)


    def get_last_batch_number(self):
        pass

    def read_log(self):
        '''
        Generator that returns each line of the file, the \n
        at the end of the line is not returned 
        '''
        with open(self._full_log_path) as log:
            for line in log:
                yield line.strip()

    # https://stackoverflow.com/questions/2301789/how-to-read-a-file-in-reverse-order
    def read_log_in_reverse(self, buf_size=8192):
        """
        A generator that returns the lines of a file in reverse order.
        Supports UTF-8 encoding
        """
        with open(self._full_log_path, 'rb') as fh:
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
                    yield line.decode()
            # Don't yield None if the file was empty
            if segment is not None:
                yield segment.decode()

    def get_recovery_operation(self):
        for line in self.read_log_in_reverse():
            line = line.split(',')
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
            line = line.split(',')
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




                

                    

