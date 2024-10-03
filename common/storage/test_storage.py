from pathlib import Path
import unittest
import storage
import os


class TestStorage(unittest.TestCase):
    def setUp(self):
        self._dir = "/tmp"
        self._range = 10

        [f.unlink() for f in Path(self._dir).glob("*") if f.is_file()]

    def tearDown(self):
        [f.unlink() for f in Path(self._dir).glob("*") if f.is_file()]

    def test_partition_is_set(self):
        app_id = 5
        record = f"{app_id},test"

        storage.write_by_range(self._dir, self._range, record)

        self.assertTrue(os.path.exists(os.path.join(os.getcwd(), self._dir)))
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    os.path.join(os.getcwd(), self._dir),
                    f"partition_{app_id // self._range}.csv",
                )
            )
        )

    def test_partition_stores_csv_correctly(self):
        app_id = 5
        original_record = f"{app_id},test"

        storage.write_by_range(self._dir, self._range, original_record)
        read_record = next(storage.read_by_range(self._dir, self._range, app_id))[0]

        self.assertEqual(original_record, read_record)

    def test_partition_stores_mutliple_csv_lines_correctly(self):
        app_id_1 = 5
        app_id_2 = 3
        original_record_1 = f"{app_id_1},test"
        original_record_2 = f"{app_id_2},test"

        storage.write_by_range(self._dir, self._range, original_record_1)
        storage.write_by_range(self._dir, self._range, original_record_2)
        records = [r for r in storage.read_by_range(self._dir, self._range, app_id_1)]

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0][0], original_record_1)
        self.assertEqual(records[1][0], original_record_2)

    def test_partition_is_set_according_to_its_key(self):
        app_id = 10
        record = f"{app_id},test"

        storage.write_by_range(self._dir, self._range, record)

        self.assertTrue(os.path.exists(os.path.join(os.getcwd(), self._dir)))
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    os.path.join(os.getcwd(), self._dir),
                    f"partition_{app_id // self._range}.csv",
                )
            )
        )

    def test_multiple_partitions_are_set(self):
        app_id_1 = 5
        app_id_2 = 10
        record_1 = f"{app_id_1},test"
        record_2 = f"{app_id_2},test"

        storage.write_by_range(self._dir, self._range, record_1)
        storage.write_by_range(self._dir, self._range, record_2)

        self.assertTrue(os.path.exists(os.path.join(os.getcwd(), self._dir)))
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    os.path.join(os.getcwd(), self._dir),
                    f"partition_{app_id_1 // self._range}.csv",
                )
            )
        )
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    os.path.join(os.getcwd(), self._dir),
                    f"partition_{app_id_2 // self._range}.csv",
                )
            )
        )

    def test_if_no_record_is_registered_for_some_key_an_empty_generator_is_returned(
        self,
    ):
        records = [r for r in storage.read_by_range(self._dir, self._range, 5)]
        self.assertEqual(len(records), 0)


if __name__ == "__main__":
    unittest.main()
