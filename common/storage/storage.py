import os
import csv
import logging
import shutil

# TODO: use threads for all functions or some parallelization tool (maybe)

def delete_directory(dir: str):
    """
    Removes the specified directory along with all its contents
    """
    if not os.path.exists(dir):
        print(f"Directory {dir} does not exist.")

        return

    shutil.rmtree(dir)
    print(f"Directory {dir} and all its contents have been removed.")
    
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


def add_to_top(dir: str, record: str, k: int):
    if k <= 0:
        logging.error("Error, K must be > 0. Got: {k}")
        return
    # TODO: modify if necessary (some records may not be a key,value pair)
    key, value = record.split(",", maxsplit=1)

    file_path = os.path.join(dir, f"top_{k}.csv")

    os.makedirs(dir, exist_ok=True)

    if not os.path.exists(file_path):
        # No existe el archivo
        # -> Crearlo y apppendear
        with open(file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow([record])
        return

    temp_file = f"temp_{k}.csv"
    top_cantidate_val = int(value)
    top_cantidate_record = record
    top_replaced = False
    top_length = 0

    with open(file_path, mode="r") as infile, open(
        temp_file, mode="w", newline=""
    ) as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Itearte over elements of the actual top
        # The first value of the top_candidate_val is the value received as parameter
        # If it's greater than a record from the top, then replace it, set the top_replaced flag to True
        # and continue. The new top_candidate_record will be the value that was replaced.
        # The top_replaced flag is to optimize the number of operations made
        for line in reader:
            if top_length == k:
                break

            # TODO: modify if necessary (some records may not be a key,value pair)
            if top_replaced:
                logging.debug(f"Shifting {line[0]} with {top_cantidate_record}")
                writer.writerow([top_cantidate_record])
                top_cantidate_record = line[0]
                top_length += 1
                continue

            read_value = int(line[0].split(",", maxsplit=1)[1])

            if read_value < top_cantidate_val:
                logging.debug(
                    f"Record: {top_cantidate_record} replaced the value: {line}"
                )
                writer.writerow([top_cantidate_record])
                top_cantidate_val = read_value
                top_cantidate_record = line[0]
                top_replaced = True
                top_length += 1
                continue

            writer.writerow(line)
            top_length += 1

        if top_length < k:
            logging.debug(f"Record {top_cantidate_record} was appended")
            writer.writerow([top_cantidate_record])
    file_path = os.path.join(dir, f"top_{k}.csv")

    os.makedirs(dir, exist_ok=True)

    # No minor element found, and top is not complete, append

    os.replace(temp_file, file_path)


def read_top(dir: str, k: int):
    if k <= 0:
        logging.error("Error, K must be > 0. Got: {k}")
        return

    file_path = os.path.join(dir, f"top_{k}.csv")

    os.makedirs(dir, exist_ok=True)

    if not os.path.exists(file_path):
        return []  # No records``

    with open(file_path, "r") as f:
        reader = csv.reader(f)
        for line in reader:
            yield line

def read_all_files(dir: str):

    if not os.path.exists(dir):
        return []  # No partitions on this dir.

    file_name_prefix = f"partition_"

    for filename in os.listdir(dir):
        if not file_name_prefix in filename:
            continue 

        file_path = os.path.join(dir, filename)

        with open(file_path, "r") as f:
            reader = csv.reader(f)
            for line in reader:
                yield line


def add_to_sorted_file(dir: str, record: str):
    #TODO: add parameter for ascending or descending order. Current order is ascending order
    #TODO: batch processing

    _, record_value = record.split(",", maxsplit=1)
    new_record_value = int(record_value)

    file_path = os.path.join(dir, f"sorted_file.csv")
    os.makedirs(dir, exist_ok=True)

    if not os.path.exists(file_path):
        with open(file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow([record])
        return

    temp_file = f"temp_sorted.csv"
    new_record_appended = False
    with open(file_path, mode="r") as infile, open(temp_file, mode="w", newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for line in reader:
            read_value = int(line[0].split(",", maxsplit=1)[1])

            if new_record_value > read_value:
                writer.writerow(line)
                continue
            
            writer.writerow([record])
            writer.writerow(line)
            new_record_appended = True

        if not new_record_appended:
            writer.writerow([record])

    os.makedirs(dir, exist_ok=True)
    os.replace(temp_file, file_path)

def read_sorted_file(dir:str):

    file_path = os.path.join(dir, "sorted_file.csv")
    os.makedirs(dir, exist_ok=True)

    if not os.path.exists(file_path):
        return []  # No records

    with open(file_path, "r") as f:
        reader = csv.reader(f)
        for line in reader:
            yield line

