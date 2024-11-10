from pathlib import Path
import unittest
from activity_log import ActivityLog
from operations import Operation, RecoveryOperation

class ActivityLogTests(unittest.TestCase):
    def setUp(self):
        self._activity_log = ActivityLog('test')
        self._dir = './log'

    def tearDown(self):
        [f.unlink() for f in Path(self._dir).glob("*") if f.is_file()]
        
    def test_01_reading_in_reverse(self):
        self._activity_log.log_begin('1')
        self._activity_log.log_write('1', ['1', '105'])
        self._activity_log.log_commit('1')

        result = []
        for line in self._activity_log.read_log_in_reverse():
            result.append(line)

        self.assertEqual(result[0], f'{Operation.COMMIT.message()},1')
        self.assertEqual(result[1], f'{Operation.WRITE.message()},1,1,105')
        self.assertEqual(result[2], f'{Operation.BEGIN.message()},1')

    def test_02_restoring_a_commited_transaction_should_redo_it(self):
        self._activity_log.log_begin('1')
        self._activity_log.log_write('1', ['1', '105'])
        self._activity_log.log_commit('1')

        recovery_operation, result_lines = self._activity_log.restore()

        self.assertEqual(recovery_operation, RecoveryOperation.REDO)
        self.assertEqual(result_lines[0], ['1', '1', '105'])


if __name__ == "__main__":
    unittest.main()