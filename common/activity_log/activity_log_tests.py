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
        self._middleware_dir = os.path.join(self._dir, 'middleware')
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
    END LOGGING
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


    def test_04_can_recover_ends_satate_correctly(self):
        client_id = '1'
        client_id_2 = '2'

        expected = {
            client_id: 2,
            client_id_2: 1
        }

        self._activity_log.log_end(client_id, '40')
        self._activity_log.log_end(client_id_2, '50')
        self._activity_log.log_end(client_id, '25')

        self.assertEqual(self._activity_log.recover_ends_state(), expected)

    def test_05_can_recover_middleware_correctly(self):
        queue_name_1 = 'test1'
        queue_name_2 = 'test2'

        msg_1 = b'quack quack quack'
        msg_2 = b'AGUANTE BOCA'
        msg_3 = b'https://music.youtube.com/watch?v=qegp2LHzGfU'

        expected = {
            queue_name_1: (msg_1, 1),
            queue_name_2: (msg_2 + msg_3, 2)
        }

        self._activity_log.log_for_middleware(queue_name_1, msg_1)
        self._activity_log.log_for_middleware(queue_name_2, msg_2)
        self._activity_log.log_for_middleware(queue_name_2, msg_3)

        state = self._activity_log.recover_middleware_state()
        
        self.assertEqual(state, expected)
    
    '''
    MIDDLEWARE LOGGING
    '''
    def verify_for_queue(self, queue_name: str, list_of_msgs: List[bytes]):
        full_path = os.path.join(self._middleware_dir, f'{queue_name}.bin')
        with open(full_path, 'rb') as log:
            data = log.read()
        
        line = 0 
        while len(data) > 0:    
            if line == len(list_of_msgs):
                raise Exception('Unexepcted amount of msgs')

            checksum = int.from_bytes(data[:CHECKSUM_LENGTH_BYTES], "big", signed=False)
            data = data[CHECKSUM_LENGTH_BYTES:]

            if checksum < CHECKSUM_LENGTH_BYTES:
                raise Exception('Checksum does not match')

            read_msg = data[:checksum]
            data = data[checksum:]

            if len(read_msg) < checksum:
                raise Exception('Checksum does not match')
            
            self.assertEqual(list_of_msgs[line], read_msg)
            line += 1
        
    def test_01_can_correctly_log_to_middleware(self):
        queue_name = 'test'
        msg = b'quack quack quack'

        self._activity_log.log_for_middleware(queue_name, msg)

        self.verify_for_queue(queue_name, [msg])


    def test_02_can_correctly_log_to_middleware_multiple_lines_for_a_single_queue(self):
        queue_name = 'test'
        msg_1 = b'quack quack quack'
        msg_2 = b'AGUANTE BOCA'

        self._activity_log.log_for_middleware(queue_name, msg_1)
        self._activity_log.log_for_middleware(queue_name, msg_2)

        self.verify_for_queue(queue_name, [msg_1, msg_2])


    def test_03_can_correctly_log_to_middleware_for_multiple_queues(self):
        queue_name_1 = 'test1'
        queue_name_2 = 'test2'

        msg_1 = b'quack quack quack'
        msg_2 = b'AGUANTE BOCA'

        self._activity_log.log_for_middleware(queue_name_1, msg_1)
        self._activity_log.log_for_middleware(queue_name_2, msg_2)

        self.verify_for_queue(queue_name_1, [msg_1])
        self.verify_for_queue(queue_name_2, [msg_2])
                
            

if __name__ == "__main__":
    unittest.main()