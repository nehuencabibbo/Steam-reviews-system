import shutil
import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from pathlib import Path
import unittest
from activity_log.activity_log import ActivityLog
from typing import * 
from .constants import * 

FIELD_LENGTH_BYTES_AMOUNT = 4
    
 
class ActivityLogTests(unittest.TestCase):
    def setUp(self):
        self._activity_log = ActivityLog()
        self._dir = './log'

    def tearDown(self):
        if Path(self._dir).exists():
            shutil.rmtree(self._dir)
            
    def test_01_can_add_processed_lines_correctly(self):
        client_id = 1
        msg_ids = ['2', '3', '5', '1', '4']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        amount_of_lines = 0
        for index, line in enumerate(self._activity_log.read_processed_lines_log(client_id)):
           amount_of_lines += 1
           self.assertEqual(str(index + 1), line[0])

        self.assertEqual(amount_of_lines, len(msg_ids))

    def test_02_can_detect_if_msg_id_has_already_been_processed(self): 
        client_id = 1
        msg_ids = ['2', '3', '5', '1', '4']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        is_present = self._activity_log.is_msg_id_already_processed('1', '2')
        self.assertTrue(is_present)

    def test_03_can_detect_if_msg_id_has_not_already_been_processed(self): 
        client_id = 1
        msg_ids = ['2', '3', '5', '1', '4']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        is_present = self._activity_log.is_msg_id_already_processed('1', '2')
        self.assertTrue(is_present)

    def test_04_can_read_general_log(self): 
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

    def test_05_already_present_processed_lines_are_skipped(self):
        client_id = 1
        msg_ids = ['2', '3', '2', '5', '4', '1', '4']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        for index, line in enumerate(self._activity_log.read_processed_lines_log(client_id)):
           self.assertEqual(str(index + 1), line)

        self.assertEqual(index + 1, 5)

    def test_06_processed_lines_supports_strings_as_values(self):
        client_id = 1
        msg_ids = ['2,W', '3,W', '2,L', '3,M', '5,L', '4,W', '4,L']
        expected = sorted(msg_ids)
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        for index, line in enumerate(self._activity_log.read_processed_lines_log(client_id)):
           self.assertEqual(line, expected[index])

    def test_07_processed_lines_correctly_detects_duplicates_when_larger_keys_are_supplied(self):
        client_id = 1
        msg_ids = ['2,W', '2,W', '2,L']
        expected = ['2,L', '2,W']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        for index, line in enumerate(self._activity_log.read_processed_lines_log(client_id)):
           self.assertEqual(line, expected[index])

if __name__ == "__main__":
    unittest.main()