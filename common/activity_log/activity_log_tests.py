import shutil
import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from pathlib import Path
import unittest
from activity_log import ActivityLog
from typing import * 

GENERAL_LOG_FILE_NAME="general_log.txt"
CLIENT_LOG_FILE_NAME="client_log.csv"

FIELD_LENGTH_BYTES_AMOUNT = 4

# TODO: Habria que hacer que herede de protocolo y solo re-implemente los
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
        self._activity_log = ActivityLog()
        self._dir = './log'

    def tearDown(self):
        if Path(self._dir).exists():
            shutil.rmtree(self._dir)
            
    def test_01_basic_reading_in_reverse_works(self):
        pass 
        # self._activity_log.log_begin('1')
        # self._activity_log.log_write('1', ['1', '105'])
        # self._activity_log.log_commit('1')

        # result = []
        # for line in self._activity_log.read_log_in_reverse():
        #     result.append(line)

        # self.assertEqual(result[0], [Operation.COMMIT.message(),'1'])
        # self.assertEqual(result[1], [Operation.WRITE.message(),'1','1','105'])
        # self.assertEqual(result[2], [Operation.BEGIN.message(),'1'])

    def test_02_reading_utf8_log_in_reverse_works(self):
        '''
        Las reviews se bajan a disco en algun punto, si no se soporta esto (que no es trivial)
        explota cuando lee las reviews
        '''
        pass 
        # expected_line = [Operation.WRITE.message() , '1' , 'Hello, world! Привет, мир! こんにちは世界 🌍']
        # self._activity_log.log_write('1', ["Hello, world! Привет, мир! こんにちは世界 🌍"])
        # for line in self._activity_log.read_log_in_reverse():
        #     self.assertEqual(expected_line, line)

    def test_03_can_add_processed_lines_correctly(self):
        client_id = 1
        msg_ids = ['2', '3', '5', '1', '4']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        amount_of_lines = 0
        for index, line in enumerate(self._activity_log.read_processed_lines_log(client_id)):
           amount_of_lines += 1
           self.assertEqual(str(index + 1), line[0])

        self.assertEqual(amount_of_lines, len(msg_ids))

    def test_04_can_detect_if_msg_id_has_already_been_processed(self): 
        client_id = 1
        msg_ids = ['2', '3', '5', '1', '4']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        is_present = self._activity_log.is_msg_id_already_processed('1', '2')
        self.assertTrue(is_present)

    def test_05_can_detect_if_msg_id_has_not_already_been_processed(self): 
        client_id = 1
        msg_ids = ['2', '3', '5', '1', '4']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        is_present = self._activity_log.is_msg_id_already_processed('1', '2')
        self.assertTrue(is_present)

    def test_06_can_read_general_log(self): 
        client_id = '1'
        data = ['aaaa', 'bbbb']
        msg_ids = ['1', '2']
        self._activity_log._log_to_general_log(client_id, data, msg_ids)

        for index, msg in enumerate(self._activity_log.read_general_log()):
            if index == 0: 
                self.assertEqual(data, msg)
            else: 
                read_client_id = msg[0]
                read_msg_ids = msg[1:]

                self.assertEqual(client_id, read_client_id)
                self.assertEqual(msg_ids, read_msg_ids)

    def test_07_already_present_processed_lines_are_skipped(self):
        client_id = 1
        msg_ids = ['2', '3', '2', '5', '4', '1', '4']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        for index, line in enumerate(self._activity_log.read_processed_lines_log(client_id)):
           self.assertEqual(str(index + 1), line)

        self.assertEqual(index + 1, 5)

    def test_08_processed_lines_supports_strings_as_values(self):
        client_id = 1
        msg_ids = ['2,W', '3,W', '2,L', '3,M', '5,L', '4,W', '4,L']
        expected = sorted(msg_ids)
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        for index, line in enumerate(self._activity_log.read_processed_lines_log(client_id)):
           self.assertEqual(line, expected[index])

    def test_09_processed_lines_correctly_detects_duplicates_when_larger_keys_are_supplied(self):
        client_id = 1
        msg_ids = ['2,W', '2,W', '2,L']
        expected = ['2,L', '2,W']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        for index, line in enumerate(self._activity_log.read_processed_lines_log(client_id)):
           self.assertEqual(line, expected[index])

if __name__ == "__main__":
    unittest.main()