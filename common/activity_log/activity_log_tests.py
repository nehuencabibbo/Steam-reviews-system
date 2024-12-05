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

        if Path(self._middleware_dir).exists():
            shutil.rmtree(self._middleware_dir)

        self._activity_log = ActivityLog()

    def tearDown(self):
        if Path(self._dir).exists():
            shutil.rmtree(self._dir)

        if Path(self._middleware_dir).exists():
            shutil.rmtree(self._middleware_dir)
    
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
        self._activity_log._log_to_general_log(client_id, data, msg_ids)

        for index, msg in enumerate(self._activity_log.read_general_log()):
            if index == 0: 
                self.assertEqual(data, msg)
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

        self.assertEqual(self._activity_log._get_amount_of_ends(client_id), 0)

        self._activity_log.log_end(client_id, '1')
        
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id), 1)

    def test_02_can_log_ends_properly_for_multiclient(self):
        client_id = '1'
        client_id_2 = '2'

        self.assertEqual(self._activity_log._get_amount_of_ends(client_id), 0)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id_2), 0)

        self._activity_log.log_end(client_id, '1')
        
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id_2), 0)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id), 1)

        self._activity_log.log_end(client_id_2, '1')

        self.assertEqual(self._activity_log._get_amount_of_ends(client_id_2), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id), 1)

    def test_03_can_properly_log_two_ends(self):
        client_id = '1'
        
        self._activity_log = ActivityLog(log_two_ends = True)

        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 0)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 0)

        self._activity_log.log_end(client_id, '20', end_logging='0')
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 0)

        self._activity_log.log_end(client_id, '21', end_logging='1')
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 1)

    def test_04_can_properly_log_two_ends_for_multiclient(self):
        # Verify for first client
        client_id = '1'
        
        self._activity_log = ActivityLog(log_two_ends = True)

        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 0)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 0)

        self._activity_log.log_end(client_id, '20', end_logging=0)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 0)

        self._activity_log.log_end(client_id, '21', end_logging=1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 1)

        #Verify for second client
        client_id_2 = '3'

        self.assertEqual(self._activity_log._get_amount_of_ends(client_id_2, end_logging='0'), 0)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id_2, end_logging='1'), 0)

        self._activity_log.log_end(client_id_2, '22', end_logging=0)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id_2, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id_2, end_logging='1'), 0)

        self._activity_log.log_end(client_id_2, '23', end_logging=1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id_2, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id_2, end_logging='1'), 1)

        # Verify that the first client still has it's ends intact 

        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 1)

    def test_05_ends_do_not_admit_duplicates_for_same_logging_type(self): 
        client_id = '1'
        msg_id = '25'

        self._activity_log = ActivityLog(log_two_ends=True)

        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 0)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 0)

        end_was_repeated = self._activity_log.log_end(client_id, msg_id, end_logging=0)

        self.assertEqual(end_was_repeated, False)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 0)
        
        end_was_repeated = self._activity_log.log_end(client_id, msg_id, end_logging=0)

        # State does not change
        self.assertEqual(end_was_repeated, True)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 0)

    def test_06_ends_admit_duplicates_if_end_logging_is_different(self):
        # msg_id can be the same for different end type
        self._activity_log = ActivityLog(log_two_ends=True)
        client_id = '1'
        msg_id = '25'

        end_was_repeated = self._activity_log.log_end(client_id, msg_id, end_logging='0')

        # State does not change
        self.assertEqual(end_was_repeated, False)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 0)

        end_was_repeated = self._activity_log.log_end(client_id, msg_id, end_logging='1')

        # State does not change
        self.assertEqual(end_was_repeated, False)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='0'), 1)
        self.assertEqual(self._activity_log._get_amount_of_ends(client_id, end_logging='1'), 1)

    '''
    RECOVERY TESTS
    '''
    def test_01_can_recover_ends_satate_correctly(self):
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

    def test_02_can_recover_middleware_correctly(self):
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

    def test_03_can_recover_for_multi_end_correctly(self):
        self._activity_log = ActivityLog(log_two_ends=True)

        client_id = '4444'
        client_id_2 = '1323'

        expected_1 = {
            client_id: 1,
            client_id_2: 4
        }  

        expected_2 = {
            client_id: 2,
            client_id_2: 3
        }

        self._activity_log.log_end(client_id, '0', end_logging='0')
        [self._activity_log.log_end(client_id_2, f'{i}', end_logging='0') for i in range(1, 5)]

        [self._activity_log.log_end(client_id, f'{i}', end_logging='1') for i in range(2)]
        [self._activity_log.log_end(client_id_2, f'{i}', end_logging='1') for i in range(2, 5)]

        first, second = self._activity_log.recover_ends_state()

        self.assertEqual(first, expected_1)
        self.assertEqual(second, expected_2)
        
    
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
                

    '''
    HANDLER LOGGING
    '''
    def test_01_con_recover_handler_state_correctly_for_empty_finished_queries(self):
        client_id = '456'
        queue_name = 'boca'
        connection_id = '1'
        last_ack_message = '4'

        finished_queries = set()

        expected = {
            client_id:
                [
                    queue_name,
                    connection_id,
                    last_ack_message,
                    finished_queries
                ]
        }

        self._activity_log.log_for_client_handler(
            client_id, 
            queue_name,
            connection_id,
            last_ack_message,
            finished_queries
        )

        recovered_state = self._activity_log.recover_client_handler_state()

        self.assertEqual(recovered_state, expected)

    def test_02_con_recover_handler_state_correctly_for_unempty_finished_queries(self):
        client_id = '456'
        queue_name = 'boca'
        connection_id = '1'
        last_ack_message = '4'

        finished_queries = set()
        [finished_queries.add(f'q{i}') for i in range(4)]

        expected = {
            client_id:
                [
                    queue_name,
                    connection_id,
                    last_ack_message,
                    finished_queries
                ]
        }

        self._activity_log.log_for_client_handler(
            client_id, 
            queue_name,
            connection_id,
            last_ack_message,
            finished_queries
        )

        recovered_state = self._activity_log.recover_client_handler_state()

        self.assertEqual(recovered_state, expected)

if __name__ == "__main__":
    unittest.main()