import os
import csv
import logging
import shutil
from typing import * 

# TODO: use threads for all functions or some parallelization tool (maybe)


def save(path: str, record: list[str]):
    with open(path, "a") as f:
        writer = csv.writer(f)
        writer.writerow(record)


def read(path: str):
    if not os.path.exists(path):
        logging.debug(f"Path {path} doesnt exists. No reviews accumulated")
        return
    with open(path, "r") as f:
        reader = csv.reader(f)
        for record in reader:
            yield record


def delete_directory(dir: str) -> bool:
    """
    Removes the specified directory along with all its contents
    """
    if not os.path.exists(dir):

        return False

    shutil.rmtree(dir)
    return True


def delete_file(file_path: str):
    if not os.path.isfile(file_path):
        logging.debug(f"Couldn't delete file {file_path}")
        return False

    os.remove(file_path)
    logging.debug(f"Deleted file {file_path}")
    return True


def write_by_range(dir: str, range: int, record: list[str]):
    key = None
    try:
        # key = int(record.split(",", maxsplit=1)[0])
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

    # key = int(record.split(",", maxsplit=1)[0])
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
            read_record = row  # [0].split(",", maxsplit=1)
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
    platform_file_name = "platform_count.csv"

    for filename in os.listdir(dir):
        if not file_name_prefix in filename and platform_file_name != filename:
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


# ------------------------ BATCHES -------------------------------------------


def _get_batch_per_client(records: List[List[str]]) -> Dict[str,List[List[str]]]:
    '''
    Given a list of records, it groups based on the first element of it
    and returns the groups.

    Ex:

    records = [['a', '1', '2'], ['b', '1', '2'], ['a', '3', '4']]

    returns:

    {'a': [['1', '2'], ['3', '4']], 'b': [['1', '2']]}
    '''
    batch_per_client = {}

    # Get the batch for every client
    for record in records:
        client_id = record[0]
        record = record[1:]
        if not client_id in batch_per_client:
            batch_per_client[client_id] = []

        batch_per_client[client_id].append(record)
    return batch_per_client


def write_batch_by_range_per_client(dir: str, range: int, records: list[list[str]]):

    batch_per_client = _get_batch_per_client(records)

    for client_id, batch in batch_per_client.items():
        client_dir = os.path.join(dir, client_id)

        _write_batch_by_range(client_dir, range, batch)


def _write_batch_by_range(dir: str, range: int, records: list[list[str]]):

    os.makedirs(dir, exist_ok=True)
    file_prefix = "partition"
    # get the file for each record in the batch -> {"file_name": [record1, record2], ....}
    records_per_file = group_by_file(file_prefix, range, records)

    for file_name, records in records_per_file.items():
        file_path = os.path.join(dir, file_name)
        with open(file_path, "a", newline="") as f:
            writer = csv.writer(f)
            for record in records:
                writer.writerow(record)


def group_by_file(
    file_prefix: str, range: int, records: list[list[str]]
) -> dict[str, list[str]]:
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

# Solamente la usa el count by platform esta funcion
def _write_batch_on_file(dir:str, file_name: str, records: list[list[Union[str, List[str]]]]):
    os.makedirs(dir, exist_ok=True)
    
    PLATFORM = 0
    MSG_IDS = 1

    #for file_name, records in records_per_file.items():
    file_path = os.path.join(dir, file_name)
    with open(file_path, "a", newline="") as f:
        writer = csv.writer(f)
        for record in records:
            platform = record[PLATFORM]
            msg_ids = record[MSG_IDS]
            count = len(msg_ids)

            writer.writerow([platform, count])


def _group_records(file_name: str, records: dict[str, List[str]]) -> Dict[str, List[str]]:
    records_per_file = {}
    for record_id, msg_id_list in records.items():
        records_per_file[file_name] = records_per_file.get(file_name, [])
        records_per_file[file_name].append([record_id, msg_id_list])

    return records_per_file

# el que usa el count by platform
def sum_platform_batch_to_records_per_client(
    dir: str, new_records_per_client: Dict[str, Dict[str, List[str]]], logger
):
    range_not_used = 0
    for client_id, new_records in new_records_per_client.items():
        # new_records = {Windows: [msg_id_1, msg_id_2, .., ], Linux: ....} 

        # client_id = numerito
        client_dir = os.path.join(dir, client_id)
        records_for_file = _group_records("platform_count.csv", new_records)
        # {platform_count.csv: [[WINDOWS, MSG_ID_LIST], [LINUX, MSG_ID_LIST]]}

        # final_path = client_dir/platform_count.csv
        _sum_batch_to_records(client_dir, range_not_used, records_for_file, logger, partition=False)

def sum_batch_to_records_per_client(
    dir: str, range: int, new_records_per_client: dict[str, dict[str, int]],
):

    for client_id, new_records in new_records_per_client.items():

        client_dir = os.path.join(dir, client_id)
        file_prefix = "partition"

        # get the file for each record in the batch -> {"file": [record1, record2], ....}
        records_per_file = _group_by_file_dict(file_prefix, range, new_records)
        # loggeo la ultima linea procesada, mensajes procesados -> 
        # loggeo que procese los mensajes -> en el archivo del cliente

        # Si se cae entre logs, al hacer la recuperacion, tiene que checkear si los mensajes
        # procesados del ultimo log estan guardados en el otro log, si ese es el caso, no
        # hay problema, pero si no estan, los tiene que agregar el
        _sum_batch_to_records(client_dir, range, records_per_file)


def _group_by_file_dict(
    file_prefix: str, range: int, records: dict[str, int]
) -> dict[str, list[str]]:
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


def create_file_if_unexistent(full_path: str):
    if not os.path.exists(full_path):
        open(full_path, 'w').close()


def _sum_batch_to_records(dir: str, range: int, records_per_file: dict[str, list[(str,int)]], logger, partition: bool = True):

    os.makedirs(dir, exist_ok=True)
    # file_prefix = "partition"

    for file_name, records in records_per_file.items():
        # file_name = partition.csv
        # records = [[WINDOWS, MSG_ID_LIST], [LINUX, MSG_ID_LIST]]
        file_path = os.path.join(dir, file_name)
        create_file_if_unexistent(file_path)

        temp_file = os.path.join(dir, f"temp_{file_name}")

        # De aca para abajo las operaciones son atomicas, o pasan o no pasan
        msg_ids_used_in_file = []
        new_file_lines = []
        with open(file_path, mode="r") as infile, open(
            temp_file, mode="w", newline=""
        ) as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            for row in reader:
                record_was_updated = False
                read_record_key = row[0]
                read_record_value = int(row[1])

                for i, record in enumerate(records):
                    # cada record es: [WINDOWS, MSG_ID_LIST]
                    # key = int(record[0]) #app_id
                    key = record[0]
                    msg_ids = record[1]
                    msg_ids_used_in_file.extend(msg_ids)
                    if read_record_key == key:
                        new_state = [read_record_key, str(read_record_value + len(msg_ids))]
                        writer.writerow(
                            new_state   
                        )
                        new_file_lines.append(','.join(new_state))
                        record_was_updated = True
                        records.pop(i)
                        break

                if not record_was_updated:
                    writer.writerow(row)
                    new_file_lines.append(','.join(row))

            for record in records:
                key = record[0]
                msg_ids = record[1]
                line = [key, str(len(msg_ids))]

                writer.writerow(line)
                new_file_lines.append(','.join(line))

        # TODO: esto lo deberia recibir por parametro y el path se deberia armar aca...
        client_id = dir.rsplit('/', maxsplit=1)[-1]
        logger.log(client_id, [file_path] + new_file_lines, msg_ids_used_in_file)

        os.replace(temp_file, file_path)

# Esta la usa el TOP
def add_batch_to_sorted_file_per_client(
    dir: str, new_records: List[List[str]], ascending: bool = True, limit: int = float("inf")
):
    logging.debug(f'NEW RECORDS: {new_records}')
    # NEW_RECORDS = [[client_id, msg_id, name, avg_playtime_forever]]
    batch_per_client = _get_batch_per_client(new_records)

    for client_id, batch in batch_per_client.items():
        client_dir = os.path.join(dir, client_id)

        _add_batch_to_sorted_file(client_dir, batch, ascending, limit)


def _add_batch_to_sorted_file(
    dir: str,
    new_records: list[str],
    ascending: bool = True,
    limit: int = float("inf"),
):
    if limit <= 0:
        logging.error(f"Error, K must be > 0. Got: {limit}")
        return
    if ascending:
        sorting_key = lambda x: (int(x[1]), x[0])
    else:
        sorting_key = lambda x: (-int(x[1]), x[0])

    sorted_records = sorted(new_records, key=sorting_key)

    file_path = os.path.join(dir, f"sorted_file.csv")
    os.makedirs(dir, exist_ok=True)

    if not os.path.exists(file_path):
        with open(file_path, "w") as f:
            writer = csv.writer(f)
            for i, record in enumerate(sorted_records):
                if i == limit:
                    break
                writer.writerow(record)

        return

    temp_file = f"temp_sorted.csv"
    amount_of_records_in_top = 0

    with open(file_path, mode="r") as infile, open(
        temp_file, mode="w", newline=""
    ) as outfile:
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
                    # new records are lower than the line
                    writer.writerow(line)
                    sorted_records = sorted_records[i:]
                    old_line_saved = True
                    break

            if not old_line_saved and amount_of_records_in_top < limit:
                # if old line was not saved, it means all new records are lower than the line
                # so i have already saved all the new records, but not updated the list
                writer.writerow(line)
                sorted_records = []
                amount_of_records_in_top += 1

        if amount_of_records_in_top < limit:
            for new_record in sorted_records:
                if amount_of_records_in_top == limit:
                    break
                # if there is at least one records left, save it here
                writer.writerow(new_record)
                amount_of_records_in_top += 1

    os.replace(temp_file, file_path)


def delete_files_from_directory(dir: str) -> bool:
    if not os.path.exists(dir):
        return False

    for filename in os.listdir(dir):

        if filename.startswith("sorted") or filename.startswith("partition"):
            file_path = os.path.join(dir, filename)
            try:
                os.remove(file_path)
            except FileNotFoundError:
                logging.error(f"{file_path} does not exist.")
