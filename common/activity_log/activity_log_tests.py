import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from pathlib import Path
import unittest
from activity_log import ActivityLog
from operations import Operation, RecoveryOperation

from typing import * 

FIELD_LENGTH_BYTES_AMOUNT = 4

# TODO: Habria que hacer que herede de protocolo i solo re-implemente los
# metodos de encode y decode, pero por alguna razon no se puede importar
# protocol a este archivo (a activity_log.py si), importa un protocol
# random que tiene que ver con cosas de matematica
class ProtocolMock():
    @staticmethod
    # Solo tiene un _ porque si se le pone __ no le hace
    # override al heredar
    def _add_checksum(message: bytes) -> bytes:
        # So that the checksum is greater than the line, simulating 
        # not written data
        length = (len(message) + 1).to_bytes(4, byteorder='big', signed=False)

        return length + message
    
    @staticmethod
    def encode(row: List[str], add_checksum=False) -> bytes:
        result = b""
        for field in row:
            encoded_field = field.encode("utf-8")
            encoded_field_length = len(encoded_field).to_bytes(
                FIELD_LENGTH_BYTES_AMOUNT, "big", signed=False
            )
            result += encoded_field_length
            result += encoded_field

        if add_checksum: 
            result = ProtocolMock._add_checksum(result)

        return result
    
    @staticmethod
    def decode(message: bytes, has_checksum=False) -> List[str]:
        if has_checksum: 
            message = Protocol.__remove_and_validate_checksum(message)

        result = []
        while len(message) > 0:
            field_length = int.from_bytes(message[:4], "big", signed=False)
            message = message[4:]

            field = message[:field_length]
            field = field.decode("utf-8")
            result.append(field)
            message = message[field_length:]

        return result
    
 
class ActivityLogTests(unittest.TestCase):
    def setUp(self):
        self._activity_log = ActivityLog('test')
        self._dir = './log'

    def tearDown(self):
        [f.unlink() for f in Path(self._dir).glob("*") if f.is_file()]
        
    def test_01_basic_reading_in_reverse_works(self):
        self._activity_log.log_begin('1')
        self._activity_log.log_write('1', ['1', '105'])
        self._activity_log.log_commit('1')

        result = []
        for line in self._activity_log.read_log_in_reverse():
            result.append(line)

        self.assertEqual(result[0], [Operation.COMMIT.message(),'1'])
        self.assertEqual(result[1], [Operation.WRITE.message(),'1','1','105'])
        self.assertEqual(result[2], [Operation.BEGIN.message(),'1'])

    def test_02_reading_utf8_log_in_reverse_works(self):
        '''
        Las reviews se bajan a disco en algun punto, si no se soporta esto (que no es trivial)
        explota cuando lee las reviews
        '''
        expected_line = [Operation.WRITE.message() , '1' , 'Hello, world! Привет, мир! こんにちは世界 🌍']
        self._activity_log.log_write('1', ["Hello, world! Привет, мир! こんにちは世界 🌍"])
        for line in self._activity_log.read_log_in_reverse():
            self.assertEqual(expected_line, line)

    def test_03_restoring_a_commited_transaction_should_redo_it(self):
        self._activity_log.log_begin('1')
        self._activity_log.log_write('1', ['1', '105'])
        self._activity_log.log_commit('1')

        recovery_operation = self._activity_log.get_recovery_operation()
        result_lines = []
        for line in self._activity_log.restore():
            result_lines.append(line)

        self.assertEqual(recovery_operation, RecoveryOperation.REDO)
        self.assertEqual(result_lines[0], ['1', '1', '105'])

    def test_04_restoring_an_uncommited_transaction_should_abort_it(self):
        self._activity_log.log_begin('1')
        self._activity_log.log_write('1', ['1', '105'])

        recovery_operation = self._activity_log.get_recovery_operation()
        result_lines = []
        for line in self._activity_log.restore():
            result_lines.append(line)

        self.assertEqual(recovery_operation, RecoveryOperation.ABORT)
        self.assertEqual(result_lines[0], ['1', '1', '105'])
    
    def test_05_restoring_a_commited_transaction_with_two_transactions_should_redo_the_last_one(self):
        self._activity_log.log_begin('1')
        self._activity_log.log_write('1', ['1', '105'])
        self._activity_log.log_write('1', ['1', '164'])
        self._activity_log.log_commit('1')
        self._activity_log.log_begin('2')
        self._activity_log.log_write('2', ['1', '25'])
        self._activity_log.log_write('2', ['2', '43'])
        self._activity_log.log_commit('2')

        recovery_operation = self._activity_log.get_recovery_operation()
        result_lines = []
        for line in self._activity_log.restore():
            result_lines.append(line)

        self.assertEqual(recovery_operation, RecoveryOperation.REDO)
        self.assertEqual(len(result_lines), 2)
        self.assertEqual(result_lines[0], ['2', '2', '43'])
        self.assertEqual(result_lines[1], ['2', '1', '25'])

    def test_06_restoring_an_uncommited_transaction_with_two_transactions_should_abort_the_last_one(self):
        self._activity_log.log_begin('1')
        self._activity_log.log_write('1', ['1', '105'])
        self._activity_log.log_write('1', ['1', '164'])
        self._activity_log.log_commit('1')
        self._activity_log.log_begin('2')
        self._activity_log.log_write('2', ['1', '25'])
        self._activity_log.log_write('2', ['2', '43'])

        recovery_operation = self._activity_log.get_recovery_operation()
        result_lines = []
        for line in self._activity_log.restore():
            result_lines.append(line)

        self.assertEqual(recovery_operation, RecoveryOperation.ABORT)
        self.assertEqual(len(result_lines), 2)
        self.assertEqual(result_lines[0], ['2', '2', '43'])
        self.assertEqual(result_lines[1], ['2', '1', '25'])

    def test_07_restoring_an_uncommited_transaction_with_a_corrupted_line_should_not_return_the_corrupted_line(self):
        mock_activity_log = ActivityLog('test', ProtocolMock())

        self._activity_log.log_begin('1')
        self._activity_log.log_write('1', ['1', '105'])
        mock_activity_log.log_write('1', ['1', '164'])

        recovery_operation = self._activity_log.get_recovery_operation()
        result_lines = []
        for line in self._activity_log.restore():
            result_lines.append(line)

        self.assertEqual(recovery_operation, RecoveryOperation.ABORT)
        self.assertEqual(len(result_lines), 1)
        self.assertEqual(result_lines[0], ['1', '1', '105'])

            
if __name__ == "__main__":
    unittest.main()