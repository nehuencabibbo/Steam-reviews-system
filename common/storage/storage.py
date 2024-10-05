import os
import csv
import logging

# TODO: use threads for all functions or some parallelization tool (maybe)


# Record is a string which format is csv. It must contain in its first value a numerical type, as it must be
# assigned to a partition of the storage (range based storage is being used)


def write_by_range(dir: str, range: int, record: str):
    key = None
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


# TODO: this could receive a batch of records
def sum_to_record(dir: str, range: int, record: str):
    key = int(record.split(",", maxsplit=1)[0])
    file_path = os.path.join(dir, f"partition_{key//range}.csv")
    os.makedirs(dir, exist_ok=True)

    if not os.path.exists(file_path):
        # No existe el archivo
        # -> Crearlo y apppendear
        write_by_range(dir, range, record)
        return

    # Existe el archivo
    # -> fijarse si esta dicha key
    #   -> Si no esta: append
    #   -> si esta: update

    temp_file = f"temp_{key//range}.csv"
    key_was_found = False

    with open(file_path, mode="r") as infile, open(
        temp_file, mode="w", newline=""
    ) as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            read_record = row[0].split(",", maxsplit=1)
            read_record_key = read_record[0]
            # TODO: validate
            read_record_value = int(read_record[1])

            if read_record_key == str(key):
                key_was_found = True
                writer.writerow([f"{read_record_key},{read_record_value+1}"])
                continue

            writer.writerow(row)
        if not key_was_found:
            writer.writerow([record])

    os.replace(temp_file, file_path)
