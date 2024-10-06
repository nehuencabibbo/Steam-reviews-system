import csv
from pathlib import Path
import unittest
import storage
import os


class TestStorage(unittest.TestCase):
    def setUp(self):
        self._dir = "./tmp"
        self._range = 10
        self._k = 3

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

    def test_sum_to_record_creates_partition_file_if_not_found(self):
        app_id = 5
        record = f"{app_id},10"

        storage.sum_to_record(self._dir, self._range, record)

        partition_path = os.path.join(
            self._dir, f"partition_{app_id // self._range}.csv"
        )
        self.assertTrue(os.path.exists(partition_path))

        read_record = next(storage.read_by_range(self._dir, self._range, app_id))[0]
        self.assertEqual(record, read_record)

    def test_sum_to_record_appends_if_key_not_found_in_existing_partition(self):
        app_id_1 = 5
        app_id_2 = 6
        record_1 = f"{app_id_1},1"
        record_2 = f"{app_id_2},1"

        storage.write_by_range(self._dir, self._range, record_1)
        storage.sum_to_record(self._dir, self._range, record_2)

        records = [
            r[0] for r in storage.read_by_range(self._dir, self._range, app_id_1)
        ]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], record_1)
        self.assertEqual(records[1], record_2)

    def test_sum_to_record_updates_existing_record(self):
        app_id = 5
        initial_record = f"{app_id},1"

        storage.sum_to_record(self._dir, self._range, initial_record)
        storage.sum_to_record(self._dir, self._range, initial_record)

        records = [r[0] for r in storage.read_by_range(self._dir, self._range, app_id)]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], f"{app_id},2")

    def test_add_to_empty_top(self):
        record = "5,10"
        top_path = os.path.join(self._dir, f"top_{self._k}.csv")

        storage.add_to_top(self._dir, record, self._k)

        self.assertTrue(os.path.exists(top_path))
        with open(top_path, "r") as f:
            reader = csv.reader(f)
            top_records = [row[0] for row in reader]
        self.assertEqual(len(top_records), 1)
        self.assertEqual(top_records[0], record)

    def test_add_to_partially_filled_top(self):
        record_1 = "5,10"
        record_2 = "6,15"
        top_path = os.path.join(self._dir, f"top_{self._k}.csv")

        storage.add_to_top(self._dir, record_1, self._k)
        storage.add_to_top(self._dir, record_2, self._k)

        self.assertTrue(os.path.exists(top_path))
        with open(top_path, "r") as f:
            reader = csv.reader(f)
            top_records = [row[0] for row in reader]
        self.assertEqual(len(top_records), 2)
        self.assertEqual(top_records[0], record_2)
        self.assertEqual(top_records[1], record_1)

    def test_add_to_fully_filled_top(self):
        record_1 = "5,10"
        record_2 = "6,15"
        record_3 = "7,5"
        new_record = "8,20"  # This record should replace the lowest (7,5)
        top_path = os.path.join(self._dir, f"top_{self._k}.csv")

        storage.add_to_top(self._dir, record_1, self._k)
        storage.add_to_top(self._dir, record_2, self._k)
        storage.add_to_top(self._dir, record_3, self._k)
        storage.add_to_top(self._dir, new_record, self._k)

        self.assertTrue(os.path.exists(top_path))
        with open(top_path, "r") as f:
            reader = csv.reader(f)
            top_records = [row[0] for row in reader]
        # The top 3 should be (in order of highest to lowest)
        self.assertEqual(len(top_records), 3)

        self.assertEqual("5,10", top_records[2])
        self.assertEqual("6,15", top_records[1])
        self.assertEqual("8,20", top_records[0])

    def test_append_if_top_is_not_filled(self):
        record_1 = "5,10"
        record_2 = "6,15"
        top_path = os.path.join(self._dir, f"top_{self._k}.csv")

        storage.add_to_top(self._dir, record_1, self._k)
        storage.add_to_top(self._dir, record_2, self._k)

        self.assertTrue(os.path.exists(top_path))
        with open(top_path, "r") as f:
            reader = csv.reader(f)
            top_records = [row[0] for row in reader]
        self.assertEqual(len(top_records), 2)

    def test_error_if_k_is_invalid(self):
        record = "5,10"
        with self.assertLogs(level="ERROR") as log:
            storage.add_to_top(self._dir, record, 0)
            self.assertIn("Error, K must be > 0", log.output[0])

    def test_top_remains_if_no_record_is_less_than_a_given_record(self):
        top_records = ["5,10", "3,20", "1,30"]
        top_file_path = os.path.join(self._dir, f"top_{self._k}")
        os.makedirs(self._dir, exist_ok=True)
        with open(top_file_path, "w", newline="") as f:
            writer = csv.writer(f)
            for record in top_records:
                writer.writerow([record])

        new_record = "7,5"
        storage.add_to_top(self._dir, new_record, self._k)

        with open(top_file_path, "r") as f:
            reader = csv.reader(f)
            top_after_add = [row[0] for row in reader]
        self.assertEqual(top_after_add, top_records)

    def test_multiple_records_are_updated_in_filled_top(self):
        top_records = [
            "1,50",
            "2,40",
            "3,30",
        ]
        top_file_path = os.path.join(self._dir, f"top_{self._k}.csv")
        for record in top_records:
            storage.add_to_top(self._dir, record, self._k)

        new_records = [
            "5,60",
            "6,45",
        ]
        for record in new_records:
            storage.add_to_top(self._dir, record, self._k)

        with open(top_file_path, "r") as f:
            reader = csv.reader(f)
            top_after_add = [row[0] for row in reader]
        expected_top = [
            "5,60",
            "1,50",
            "6,45",
        ]
        self.assertEqual(top_after_add, expected_top)

    def test_read_from_all_partitions(self):
        app_id_1 = 5
        app_id_2 = 10
        original_record_1 = f"{app_id_1},test"
        original_record_2 = f"{app_id_2},test"

        storage.write_by_range(self._dir, self._range, original_record_1)
        storage.write_by_range(self._dir, self._range, original_record_2)
        records = [r for r in storage.read_all_files(self._dir)]

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0][0], original_record_2)
        self.assertEqual(records[1][0], original_record_1)


    def test_add_to_sorted_file_creates_file(self):
        record = "5,10"
        top_path = os.path.join(self._dir, f"sorted_file.csv")

        storage.add_to_sorted_file(self._dir, record)
        self.assertTrue(os.path.exists(top_path))

    def test_add_to_sorted_file(self):
        record = "5,10"

        storage.add_to_sorted_file(self._dir, record)

        reader = storage.read_sorted_file(self._dir)
        read_records = [row[0] for row in reader]

        self.assertEqual(len(read_records), 1)
        self.assertEqual(read_records[0], record)

    def test_add_to_sorted_file_with_multiple_records(self):
        records = ["5,10", "5,50", "5,20"]

        for record in records:
            storage.add_to_sorted_file(self._dir, record)

        reader = storage.read_sorted_file(self._dir)
        read_records = [row[0] for row in reader]

        self.assertEqual(len(read_records), 3)
        self.assertEqual(read_records[0], records[0])
        self.assertEqual(read_records[2], records[1])
        self.assertEqual(read_records[1], records[2])

if __name__ == "__main__":
    unittest.main()
