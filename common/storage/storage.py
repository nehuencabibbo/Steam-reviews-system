import os
import csv

# TODO: use threads for all functions or some parallelization tool (maybe)


# Record is a string which format is csv. It must contain in its first value a numerical type, as it must be
# assigned to a partition of the storage (range based storage is being used)
def write_by_range(dir: str, range: int, record: str):
    try:
        key = int(record.split(",", maxsplit=1)[0])
        file_path = os.path.join(dir, f"partition_{key//range}.csv")
        os.makedirs(dir, exist_ok=True)

        with open(file_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([record])

    except ValueError as e:
        print(f"Received {key}, Expected a numerical type in its place")
        raise e


def read_by_range(dir: str, range: int, key: int):
    file_name = f"partition_{key//range}.csv"

    file_path = os.path.join(dir, file_name)

    if not os.path.exists(file_path):
        return []  # No records for this key.

    with open(file_path, "r") as f:
        reader = csv.reader(f)
        for line in reader:
            yield line
