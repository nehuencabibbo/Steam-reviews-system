import os
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
        self._dir = './log'
        if Path(self._dir).exists():
            shutil.rmtree(self._dir)

        self._activity_log = ActivityLog()

    def tearDown(self):
        if Path(self._dir).exists():
            shutil.rmtree(self._dir)
    
    '''
    PROCESED LINES TESTS
    '''
    def test_01_can_add_processed_lines_correctly(self):
        client_id = '1'
        msg_ids = ['2', '3', '5', '1', '4']
        for msg_id in msg_ids:
            self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), False)

        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)
        
        for msg_id in msg_ids:
            self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), True)

    def test_02_can_add_processed_lines_correctly_for_multiclient(self):
        client_id = '1'
        msg_ids = ['2', '3', '5', '1', '4']
        for msg_id in msg_ids:
            self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), False)

        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)
        
        for msg_id in msg_ids:
            self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), True)


        client_id = '2'
        new_msg_ids = ['1040', '1200', '1300']
        for msg_id in msg_ids:
            self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), False)

        for msg_id in new_msg_ids:
            self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), False)

        self._activity_log._log_to_processed_lines(client_id, new_msg_ids)
        
        for msg_id in new_msg_ids:
            self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), True)

    def test_03_already_present_processed_lines_are_skipped(self):
        client_id = '1'
        msg_ids = ['2', '2']
        for msg_id in msg_ids:
            self._activity_log._log_to_processed_lines(client_id, msg_id)

        file_name = self._activity_log._get_partition_file_name(int(msg_id[0]))
        path = os.path.join(self._dir, client_id, file_name)

        with open(path, 'r') as log: 
            count = 0 
            for line in log:
                self.assertEqual(line.strip(), msg_id[0])
                count += 1
            
            self.assertEqual(count, 1)
    
    '''
    GENERAL LOG TESTS
    '''
    def test_01_can_read_general_log(self): 
        client_id = '1'
        data = ['aaaa', 'bbbb']
        msg_ids = ['1', '2']
        self._activity_log._log_to_general_log(client_id, data, msg_ids, GENERAL_LOGGING)

        for index, msg in enumerate(self._activity_log.read_general_log()):
            if index == 0: 
                self.assertEqual([GENERAL_LOGGING] + data, msg)
            else: 
                read_client_id = msg[0]
                read_msg_ids = msg[1:]

                self.assertEqual(client_id, read_client_id)
                self.assertEqual(msg_ids, read_msg_ids)

    '''
    LOG ENDS TESTS
    '''
    def test_01_can_log_ends_properly(self):
        client_id = '1'

        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 0)

        self._activity_log.log_end(client_id, '1')
        
        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 1)

    def test_02_can_log_ends_properly_for_multiclient(self):
        client_id = '1'
        client_id_2 = '2'

        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 0)
        self.assertEqual(self._activity_log.get_amount_of_ends(client_id_2), 0)

        self._activity_log.log_end(client_id, '1')
        
        self.assertEqual(self._activity_log.get_amount_of_ends(client_id_2), 0)
        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 1)

        self._activity_log.log_end(client_id_2, '1')

        self.assertEqual(self._activity_log.get_amount_of_ends(client_id_2), 1)
        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 1)

    '''
    RECOVERY TESTS
    '''
    def test_01_loging_end_and_then_recovering_does_not_modify_the_state_of_end_or_processed_lines(self):
        client_id = '1'
        msg_id = '14700'

        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 0)
        self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), False)

        self._activity_log.log_end(client_id, msg_id)

        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 1)
        self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), True)

        file_name, state = self._activity_log.recover()

        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 1)
        self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), True)

        self.assertEqual(file_name, None)
        self.assertEqual(state, None)


    def test_02_recovering_when_last_operation_was_end_updates_end_and_procesed_lines_to_new_state(self):
        client_id = '745'
        new_state = [
            self._activity_log._get_ends_file_path(client_id),
            '5'
        ]
        msg_id = ['15600']
        self._activity_log._log_to_general_log(
            client_id,
            new_state,
            msg_id,
            END_LOGGING
        )
        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 0)
        self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id[0]), False)

        file_name, state = self._activity_log.recover()

        self.assertEqual(file_name, None)
        self.assertEqual(state, None)

        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), int(new_state[1]))
        self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id[0]), True)


    def test_03_recovering_when_last_operation_was_not_end_leaves_end_file_as_it_was_and_recovers_processed_lines(self):
        client_id = '745'
        new_state = [
            'some_random_path.txt',
            'quack'
        ]
        msg_ids = ['15600', '12342', '122', '123', '124']
        self._activity_log._log_to_general_log(
            client_id,
            new_state,
            msg_ids,
            GENERAL_LOGGING
        )
        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 0)
        for msg_id in msg_ids: 
            self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), False)

        file_name, state = self._activity_log.recover()

        self.assertEqual(file_name, new_state[0])
        self.assertEqual(state, [new_state[1]])

        for msg_id in msg_ids: 
            self.assertEqual(self._activity_log.is_msg_id_already_processed(client_id, msg_id), True)

        self.assertEqual(self._activity_log.get_amount_of_ends(client_id), 0)

            

if __name__ == "__main__":
    unittest.main()