import os
import csv
import logging
import shutil

# TODO: use threads for all functions or some parallelization tool (maybe)

def delete_directory(dir: str) -> bool:
    """
    Removes the specified directory along with all its contents
    """
    if not os.path.exists(dir):

        return False 

    shutil.rmtree(dir)
    return True 
    
def write_by_range(dir: str, range: int, record: list[str]):
    key = None
    try:
        #key = int(record.split(",", maxsplit=1)[0])
        key = int(record[0])
        file_path = os.path.join(dir, f"partition_{key//range}.csv")
        os.makedirs(dir, exist_ok=True)

        with open(file_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(record)

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
def sum_to_record(dir: str, range: int, record: list[str]):

    #key = int(record.split(",", maxsplit=1)[0])
    key = int(record[0])
    value = int(record[1])

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
            read_record = row #[0].split(",", maxsplit=1)
            read_record_key = read_record[0]
            # TODO: validate
            read_record_value = int(read_record[1])

            if read_record_key == str(key):
                key_was_found = True
                writer.writerow([read_record_key, read_record_value + value])
                continue

            writer.writerow(row)
        if not key_was_found:
            writer.writerow(record)

    os.replace(temp_file, file_path)


def add_to_top(dir: str, record: list[str], k: int):
    if k <= 0:
        logging.error("Error, K must be > 0. Got: {k}")
        return
    # TODO: modify if necessary (some records may not be a key,value pair)
    key, value = record

    file_path = os.path.join(dir, f"top_{k}.csv")

    os.makedirs(dir, exist_ok=True)

    if not os.path.exists(file_path):
        # No existe el archivo
        # -> Crearlo y apppendear
        with open(file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow(record)
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
                logging.debug(f"Shifting {line} with {top_cantidate_record}")
                writer.writerow(top_cantidate_record)
                top_cantidate_record = line
                top_length += 1
                continue
            
            read_name, read_value = line
            read_value = int(read_value)

            if read_value == top_cantidate_val:
                logging.debug(
                    f"Record: {top_cantidate_record} has the same value than: {line}"
                )
                logging.debug(f"{key} > {read_name}?")
                if key > read_name:
                    logging.debug(
                        f"Record: {top_cantidate_record} replaced the value: {line}"
                    )
                    writer.writerow(top_cantidate_record)
                    top_cantidate_val = read_value
                    top_cantidate_record = line
                    top_replaced = True
                    top_length += 1
                else:
                    writer.writerow(line)
                    top_length += 1

                # continue anyways as it has to check if the name is greater than other names
                continue

            if read_value < top_cantidate_val:
                logging.debug(
                    f"Record: {top_cantidate_record} replaced the value: {line}"
                )
                writer.writerow(top_cantidate_record)
                top_cantidate_val = read_value
                top_cantidate_record = line
                top_replaced = True
                top_length += 1
                continue

            writer.writerow(line)
            top_length += 1

        if top_length < k:
            logging.debug(f"Record {top_cantidate_record} was appended")
            writer.writerow(top_cantidate_record)

    file_path = os.path.join(dir, f"top_{k}.csv")

    os.makedirs(dir, exist_ok=True)

    # No minor element found, and top is not complete, append

    os.replace(temp_file, file_path)

    # [logging.debug(val) for val in read_top(dir, k)]

# en batches se usa el read_sorted_file
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
    # TODO: add parameter for ascending or descending order. Current order is ascending order
    # TODO: batch processing
    record_name = record[0]
    new_record_value = int(record[1])

    file_path = os.path.join(dir, f"sorted_file.csv")
    os.makedirs(dir, exist_ok=True)

    if not os.path.exists(file_path):
        with open(file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow(record)
        return

    temp_file = f"temp_sorted.csv"
    new_record_appended = False
    with open(file_path, mode="r") as infile, open(
        temp_file, mode="w", newline=""
    ) as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for line in reader:
            read_name = line[0]
            read_value = int(line[1])

            if new_record_value > read_value:
                writer.writerow(line)
                continue
            elif new_record_value == read_value and record_name >= read_name:
                writer.writerow(line)
                continue

            if not new_record_appended:
                writer.writerow(record)
            writer.writerow(line)
            new_record_appended = True

        if not new_record_appended:
            writer.writerow(record)

    os.makedirs(dir, exist_ok=True)
    os.replace(temp_file, file_path)


def read_sorted_file(dir: str):

    file_path = os.path.join(dir, "sorted_file.csv")
    os.makedirs(dir, exist_ok=True)

    if not os.path.exists(file_path):
        return []  # No records

    with open(file_path, "r") as f:
        reader = csv.reader(f)
        for line in reader:
            yield line

#------------------------ BATCHES -------------------------------------------

def group_by_file(file_prefix:str, range:int, records: list[list[str]]) -> dict[str, list[str]]: 
    records_per_file = {}
    for record in records:
        try:
            key = int(record[0])
            file_name = f"{file_prefix}_{key//range}.csv"
            records_per_file[file_name] = records_per_file.get(file_name, [])
            records_per_file[file_name].append(record)
        except ValueError as e:
            print(f"Received {key}, Expected a numerical type in its place")
            raise e
    
    return records_per_file


def write_batch_by_range(dir: str, range: int, records: list[list[str]]):

    os.makedirs(dir, exist_ok=True)
    file_prefix = "partition"
    #get the file for each record in the batch -> {"file_name": [record1, record2], ....}
    records_per_file = group_by_file(file_prefix, range, records)

    for file_name, records in records_per_file.items():
        file_path = os.path.join(dir, file_name)
        with open(file_path, "a", newline="") as f:
            writer = csv.writer(f)
            for record in records:
                writer.writerow(record)


def group_by_file_dict(file_prefix:str, range:int, records: dict[str, int]) -> dict[str, list[str]]: 
    records_per_file = {}
    for record_id, value in records.items():
        try:
            key = int(record_id)
            file_name = f"{file_prefix}_{key//range}.csv"
            records_per_file[file_name] = records_per_file.get(file_name, [])
            records_per_file[file_name].append([record_id, value])
        except ValueError as e:
            print(f"Received {key}, Expected a numerical type in its place")
            raise e
    
    return records_per_file

#TODO: receive a dict instead of a list
def sum_batch_to_records(dir: str, range: int, new_records: dict[str, int]):

    os.makedirs(dir, exist_ok=True)
    file_prefix = "partition"

    #get the file for each record in the batch -> {"file": [record1, record2], ....}
    records_per_file = group_by_file_dict(file_prefix, range, new_records)

    for file_name, records in records_per_file.items():
        file_path = os.path.join(dir, file_name) 
        if not os.path.exists(file_path):
            write_batch_by_range(dir, range, records)
            continue
        
        temp_file = os.path.join(dir, f"temp_{file_name}")

        with open(file_path, mode="r") as infile, open(temp_file, mode="w", newline="") as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            for row in reader:
                record_was_updated = False
                read_record_key = row[0]
                read_record_value = int(row[1])

                for i, record in enumerate(records):
                    key = int(record[0])
                    if read_record_key == str(key):
                        writer.writerow([read_record_key, read_record_value + int(record[1])])
                        record_was_updated = True
                        records.pop(i)
                        break
                if not record_was_updated:
                    writer.writerow(row)

            for record in records:
                writer.writerow(record)

        os.replace(temp_file, file_path)


def add_batch_to_sorted_file(dir: str, new_records: str, ascending: bool = True, limit: int = float('inf')):
    if limit <= 0:
        logging.error(f"Error, K must be > 0. Got: {limit}")
        return
    
    if ascending: sorting_key = lambda x: (int(x[1]), x[0])
    else: sorting_key = lambda x: (-int(x[1]), x[0])

    sorted_records = sorted(new_records, key=sorting_key)
    
    file_path = os.path.join(dir, f"sorted_file.csv")
    os.makedirs(dir, exist_ok=True)

    if not os.path.exists(file_path):
        with open(file_path, "w") as f:
            writer = csv.writer(f)
            for i, record in enumerate(sorted_records):
                if i == limit: break
                writer.writerow(record)

        return

    temp_file = f"temp_sorted.csv"
    amount_of_records_in_top = 0

    with open(file_path, mode="r") as infile, open(temp_file, mode="w", newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for line in reader:
            if amount_of_records_in_top == limit:
                break
            old_line_saved = False

            read_name = line[0]
            read_value = int(line[1])

            if not ascending: 
                read_value = -read_value
            
            for i, new_record in enumerate(sorted_records):
                if amount_of_records_in_top == limit:
                    break

                new_record_name = new_record[0]
                new_record_value = int(new_record[1])

                if not ascending: 
                    new_record_value = -new_record_value
                
                amount_of_records_in_top += 1

                if new_record_value < read_value:
                    writer.writerow(new_record)
                elif new_record_value == read_value and new_record_name < read_name:
                    writer.writerow(new_record)
                else:
                    #new records are lower than the line
                    writer.writerow(line)
                    sorted_records = sorted_records[i:]
                    old_line_saved = True
                    break

            if not old_line_saved and amount_of_records_in_top < limit:
                #if old line was not saved, it means all new records are lower than the line
                #so i have already saved all the new records, but not updated the list
                writer.writerow(line)
                sorted_records = []
                amount_of_records_in_top += 1

        if amount_of_records_in_top < limit:
            for new_record in sorted_records:
                if amount_of_records_in_top == limit:
                    break
                #if there is at least one records left, save it here
                writer.writerow(new_record)
                amount_of_records_in_top += 1

    os.replace(temp_file, file_path)


def delete_files_from_directory(dir: str) -> bool:
    if not os.path.exists(dir):
        return False 

    for filename in os.listdir(dir):

        if filename.startswith('sorted') or filename.startswith('partition'):
            file_path = os.path.join(dir, filename)
            try:
                os.remove(file_path)
            except FileNotFoundError:
                logging.error(f"{file_path} does not exist.")