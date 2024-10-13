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
        record = ["5", "test"]

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
        original_record = ["5", "test"]

        storage.write_by_range(self._dir, self._range, original_record)
        read_record = next(storage.read_by_range(self._dir, self._range, app_id))

        self.assertEqual(original_record, read_record)

    def test_partition_stores_mutliple_csv_lines_correctly(self):
        app_id_1 = 5
        original_record_1 = ["5", "test"]
        original_record_2 = ["3", "test"]

        storage.write_by_range(self._dir, self._range, original_record_1)
        storage.write_by_range(self._dir, self._range, original_record_2)
        records = [r for r in storage.read_by_range(self._dir, self._range, app_id_1)]

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], original_record_1)
        self.assertEqual(records[1], original_record_2)

    def test_partition_is_set_according_to_its_key(self):
        app_id = 10
        record =["10", "test"]

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
        record_1 = ["5", "test"]
        record_2 = ["10", "test"]

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
        record = ["5", "10"]

        storage.sum_to_record(self._dir, self._range, record)

        partition_path = os.path.join(
            self._dir, f"partition_{app_id // self._range}.csv"
        )
        self.assertTrue(os.path.exists(partition_path))

        read_record = next(storage.read_by_range(self._dir, self._range, app_id))
        self.assertEqual(record, read_record)

    def test_sum_to_record_appends_if_key_not_found_in_existing_partition(self):
        app_id_1 = 5
        record_1 = ["5", "1"]
        record_2 = ["6", "1"]

        storage.write_by_range(self._dir, self._range, record_1)
        storage.sum_to_record(self._dir, self._range, record_2)

        records = [
            r for r in storage.read_by_range(self._dir, self._range, app_id_1)
        ]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], record_1)
        self.assertEqual(records[1], record_2)

    def test_sum_to_record_updates_existing_record(self):
        app_id = 5
        initial_record = ["5", "1"]

        storage.sum_to_record(self._dir, self._range, initial_record)
        storage.sum_to_record(self._dir, self._range, initial_record)

        records = [r for r in storage.read_by_range(self._dir, self._range, app_id)]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], ["5", "2"])

    def test_add_to_empty_top(self):
        record = ["5","10"]
        top_path = os.path.join(self._dir, f"top_{self._k}.csv")

        storage.add_to_top(self._dir, record, self._k)

        self.assertTrue(os.path.exists(top_path))
        with open(top_path, "r") as f:
            reader = csv.reader(f)
            top_records = [row for row in reader]
        self.assertEqual(len(top_records), 1)
        self.assertEqual(top_records[0], record)

    def test_add_to_partially_filled_top(self):
        record_1 = ["5","10"]
        record_2 = ["6","15"]
        top_path = os.path.join(self._dir, f"top_{self._k}.csv")

        storage.add_to_top(self._dir, record_1, self._k)
        storage.add_to_top(self._dir, record_2, self._k)

        self.assertTrue(os.path.exists(top_path))
        with open(top_path, "r") as f:
            reader = csv.reader(f)
            top_records = [row for row in reader]
        self.assertEqual(len(top_records), 2)
        self.assertEqual(top_records[0], record_2)
        self.assertEqual(top_records[1], record_1)

    def test_add_to_fully_filled_top(self):
        record_1 = ["5","10"]
        record_2 = ["6","15"]
        record_3 = ["7","5"]
        new_record = ["8","20"]  # This record should replace the lowest (7,5)
        top_path = os.path.join(self._dir, f"top_{self._k}.csv")

        storage.add_to_top(self._dir, record_1, self._k)
        storage.add_to_top(self._dir, record_2, self._k)
        storage.add_to_top(self._dir, record_3, self._k)
        storage.add_to_top(self._dir, new_record, self._k)

        self.assertTrue(os.path.exists(top_path))
        with open(top_path, "r") as f:
            reader = csv.reader(f)
            top_records = [row for row in reader]
        # The top 3 should be (in order of highest to lowest)
        self.assertEqual(len(top_records), 3)

        self.assertEqual(["5","10"], top_records[2])
        self.assertEqual(["6","15"], top_records[1])
        self.assertEqual(["8","20"], top_records[0])

    def test_append_if_top_is_not_filled(self):
        record_1 = ["5","10"]
        record_2 = ["6","15"]
        top_path = os.path.join(self._dir, f"top_{self._k}.csv")

        storage.add_to_top(self._dir, record_1, self._k)
        storage.add_to_top(self._dir, record_2, self._k)

        self.assertTrue(os.path.exists(top_path))
        with open(top_path, "r") as f:
            reader = csv.reader(f)
            top_records = [row[0] for row in reader]
        self.assertEqual(len(top_records), 2)

    def test_error_if_k_is_invalid(self):
        record = ["5","10"]
        with self.assertLogs(level="ERROR") as log:
            storage.add_to_top(self._dir, record, 0)
            self.assertIn("Error, K must be > 0", log.output[0])

    def test_top_remains_if_no_record_is_less_than_a_given_record(self):
        top_records = [["5","10"], ["3","20"], ["1","30"]]
        top_file_path = os.path.join(self._dir, f"top_{self._k}")
        os.makedirs(self._dir, exist_ok=True)
        with open(top_file_path, "w", newline="") as f:
            writer = csv.writer(f)
            for record in top_records:
                writer.writerow(record)

        new_record = ["7","5"]
        storage.add_to_top(self._dir, new_record, self._k)

        with open(top_file_path, "r") as f:
            reader = csv.reader(f)
            top_after_add = [row for row in reader]
        self.assertEqual(top_after_add, top_records)

    def test_multiple_records_are_updated_in_filled_top(self):
        top_records = [
            ["1","50"],
            ["2","40"],
            ["3","30"],
        ]
        top_file_path = os.path.join(self._dir, f"top_{self._k}.csv")
        for record in top_records:
            storage.add_to_top(self._dir, record, self._k)

        new_records = [
            ["5","60"],
            ["6","45"],
        ]
        for record in new_records:
            storage.add_to_top(self._dir, record, self._k)

        with open(top_file_path, "r") as f:
            reader = csv.reader(f)
            top_after_add = [row for row in reader]
        expected_top = [
            ["5","60"],
            ["1","50"],
            ["6","45"],
        ]
        self.assertEqual(top_after_add, expected_top)

    def test_read_from_all_partitions(self):
        original_record_1 = ["5", "test"]
        original_record_2 = ["10", "test"]

        storage.write_by_range(self._dir, self._range, original_record_1)
        storage.write_by_range(self._dir, self._range, original_record_2)
        records = [r for r in storage.read_all_files(self._dir)]

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], original_record_2)
        self.assertEqual(records[1], original_record_1)


    def test_add_to_sorted_file_creates_file(self):
        record = ["5", "10"]
        top_path = os.path.join(self._dir, f"sorted_file.csv")

        storage.add_to_sorted_file(self._dir, record)
        self.assertTrue(os.path.exists(top_path))

    def test_add_to_sorted_file(self):
        record = ["5", "10"]

        storage.add_to_sorted_file(self._dir, record)

        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]

        self.assertEqual(len(read_records), 1)
        self.assertEqual(read_records[0], record)

    def test_add_to_sorted_file_with_multiple_records(self):
        records = [["5", "10"], ["5", "50"], ["5", "20"]]

        for record in records:
            storage.add_to_sorted_file(self._dir, record)

        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]

        self.assertEqual(len(read_records), 3)
        self.assertEqual(read_records[0], records[0])
        self.assertEqual(read_records[2], records[1])
        self.assertEqual(read_records[1], records[2])

    def test_add_to_sorted_storage_when_values_are_equal(self):
        records = [["b", "10"], ["a", "10"], ["c", "10"]]

        for record in records:
            storage.add_to_sorted_file(self._dir, record)

        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]

        self.assertEqual(len(read_records), 3)
        self.assertEqual(read_records[0], records[1])
        self.assertEqual(read_records[1], records[0])
        self.assertEqual(read_records[2], records[2])

# ------------------------ BATCH TEST -----------------------------

    def test_write_by_range_with_batches_one_partition(self):
        partition_app_id = 1
        batch = [["1", "test"], ["3", "test"]]

        storage.write_batch_by_range(self._dir, self._range, batch)

        records = [r for r in storage.read_by_range(self._dir, self._range, partition_app_id)]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], batch[0])
        self.assertEqual(records[1], batch[1])

    def test_write_by_range_with_batches_multiple_partition(self):
        partition1_app_id = 1
        partition2_app_id = 10
        batch = [["1", "test"], ["10", "test"]]

        storage.write_batch_by_range(self._dir, self._range, batch)

        records_partition_1 = [r for r in storage.read_by_range(self._dir, self._range, partition1_app_id)]
        records_partition_2 = [r for r in storage.read_by_range(self._dir, self._range, partition2_app_id)]

        self.assertEqual(len(records_partition_1), 1)
        self.assertEqual(len(records_partition_2), 1)
        self.assertEqual(records_partition_1[0], batch[0])
        self.assertEqual(records_partition_2[0], batch[1])

    def test_sum_batch_to_record_one_partition(self):
        partition_key = 5
        batch = [["5", "1"], ["6", "1"]]

        storage.sum_batch_to_records(self._dir, self._range,batch)
        records = [r for r in storage.read_by_range(self._dir, self._range, partition_key)]

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], batch[0])
        self.assertEqual(records[1], batch[1])

    def test_sum_batch_to_record_update_record(self):
        partition_key = 5
        batch1 = [["5", "1"], ["6", "1"]]
        batch2 = [["5", "2"], ["7", "1"]]

        storage.sum_batch_to_records(self._dir, self._range, batch1)
        storage.sum_batch_to_records(self._dir, self._range, batch2)

        records = [r for r in storage.read_by_range(self._dir, self._range, partition_key)]

        self.assertEqual(len(records), 3)
        self.assertEqual(records[0], ["5", "3"])
        self.assertEqual(records[1], batch1[1])
        self.assertEqual(records[2], batch2[1])

    def test_sum_batch_to_record_one_record(self):
        partition_key = 5
        batch = [["5", "1"]]

        storage.sum_batch_to_records(self._dir, self._range,batch)
        storage.sum_batch_to_records(self._dir, self._range,batch)
        records = [r for r in storage.read_by_range(self._dir, self._range, partition_key)]

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], ["5", "2"])



    def test_add_batch_to_sorted_file_with_one_batch(self):
        records = [["5", "10"], ["5", "50"], ["5", "20"]]

        storage.add_batch_to_sorted_file(self._dir, records)

        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]

        self.assertEqual(len(read_records), 3)
        self.assertEqual(read_records[0], records[0])
        self.assertEqual(read_records[2], records[1])
        self.assertEqual(read_records[1], records[2])

    def test_add_batch_to_sorted_storage_when_values_are_equal(self):
        records = [["b", "10"], ["a", "10"], ["c", "10"]]

        storage.add_batch_to_sorted_file(self._dir, records)

        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]

        self.assertEqual(len(read_records), 3)
        self.assertEqual(read_records[0], records[1])
        self.assertEqual(read_records[1], records[0])
        self.assertEqual(read_records[2], records[2])

    def test_add_batch_to_sorted_storage_multiple_batches(self):
        records1 = [["b", "10"], ["z", "11"]]
        records2 = [["c", "11"], ["a", "4"]]

        storage.add_batch_to_sorted_file(self._dir, records1)
        storage.add_batch_to_sorted_file(self._dir, records2)

        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]

        self.assertEqual(len(read_records), 4)
        self.assertEqual(read_records[0], records2[1])
        self.assertEqual(read_records[1], records1[0])
        self.assertEqual(read_records[2], records2[0])
        self.assertEqual(read_records[3], records1[1])


    def test_add_batch_to_empty_top(self):
        batch = [["5","10"]]
        #top_path = os.path.join(self._dir, f"top_{self._k}.csv")

        #storage.add_batch_to_top(self._dir, batch, self._k)
        storage.add_batch_to_sorted_file(self._dir, batch, ascending=False, limit=self._k)
        
        #reader = storage.read_top(self._dir, self._k)
        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]

        self.assertEqual(len(read_records), 1)
        self.assertEqual(read_records[0], batch[0])

    def test_add_batch_to_partially_filled_top(self): 
        batch1 = [["5","10"]]
        batch2 = [["6","15"]]

        storage.add_batch_to_sorted_file(self._dir, batch1, ascending=False, limit=self._k)
        storage.add_batch_to_sorted_file(self._dir, batch2, ascending=False, limit=self._k)
        
        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]

        self.assertEqual(len(read_records), 2)
        self.assertEqual(read_records[0], batch2[0])
        self.assertEqual(read_records[1], batch1[0])

    def test_add_batch_to_top_when_batch_len_is_higher_than_k(self):
        batch = [["5","10"], ["6","15"], ["7","5"], ["8","20"]] 

        storage.add_batch_to_sorted_file(self._dir, batch, ascending=False, limit=self._k)
        
        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]

        # The top 3 should be (in order of highest to lowest)
        self.assertEqual(len(read_records), 3)

        self.assertEqual(["5","10"], read_records[2])
        self.assertEqual(["6","15"], read_records[1])
        self.assertEqual(["8","20"], read_records[0])

    def test_error_for_batch_top_if_k_is_invalid(self):
        batch = [["5","10"]]

        with self.assertLogs(level="ERROR") as log:
            storage.add_batch_to_sorted_file(self._dir, batch, ascending=False, limit=0)
            self.assertIn("Error, K must be > 0", log.output[0])

    def test_top_remains_if_no_record_in_batch_is_less_than_a_given_record(self):
        batch = [["1","30"], ["3","20"], ["5","10"]]
        new_batch = [["7","5"], ["9", "2"]]

        storage.add_batch_to_sorted_file(self._dir, batch, ascending=False, limit=self._k)
        storage.add_batch_to_sorted_file(self._dir, new_batch, ascending=False, limit=self._k)

        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]
        
        self.assertEqual(read_records, batch)

    def test_multiple_records_are_updated_in_filled_top_whit_new_batch(self):
        top_records = [["1","50"], ["2","40"], ["3","30"]]
        new_records = [["5","60"], ["6","45"]]

        storage.add_batch_to_sorted_file(self._dir, top_records, ascending=False, limit=self._k)
        storage.add_batch_to_sorted_file(self._dir, new_records, ascending=False, limit=self._k)

        reader = storage.read_sorted_file(self._dir)
        read_records = [row for row in reader]

        expected_top = [["5","60"], ["1","50"], ["6","45"]]
        self.assertEqual(len(read_records), 3)
        self.assertEqual(read_records, expected_top)


if __name__ == "__main__":
    unittest.main()
