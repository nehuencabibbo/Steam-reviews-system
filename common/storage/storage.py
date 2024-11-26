import os
import csv
import logging
import shutil
from typing import *

from utils.utils import group_batch_by_field
    

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


def read_by_range(dir: str, range: int, key: int):
    logging.debug(f'READING KEY: {key}, RANGE: {range}')
    file_name = f"partition_{key//range}.csv"

    file_path = os.path.join(dir, file_name)

    if not os.path.exists(file_path):
        return []  # No records for this key.

    with open(file_path, "r") as f:
        reader = csv.reader(f)
        for line in reader:
            yield line


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
    os.makedirs(dir, exist_ok=True)  # ???

    if not os.path.exists(file_path):
        return []  # No records

    with open(file_path, "r") as f:
        reader = csv.reader(f)
        for line in reader:
            yield line


# ------------------------ BATCHES -------------------------------------------


# TODO: Sacar el group_batch_by_field_de_aca
def write_batch_by_range_per_client(dir: str, range: int, records: List[List[str]], key_index: int):
    records_per_client = group_batch_by_field(records)
    # logging.debug(f'Records per client: {records_per_client}')
    for client_id, batch in records_per_client.items():
        client_dir = os.path.join(dir, client_id)

        _write_batch_by_range(client_dir, range, batch, key_index)


def atomically_append_to_file(dir: str, file_name: str, records: List[List[str]]):
    '''
    Records need to have the following format: 

        [
            [UNIQUE_IDENTIFIER, ...],
            ...
        ]
    
    If a unique identifier (for ANY record) is present in the corresponding file, 
    then the whole append operation will be aborted, as that indicates that this 
    list of record has already been processed completly 
    '''
    UNIQUE_IDENTIFIER_INDEX = 0
    # If file is large this is really ineficient
    temp_file_path = os.path.join(dir, 'temp_append.csv')
    file_path = os.path.join(dir, file_name)

    create_file_if_unexistent(file_path)
    found_duplicate = False
    with open(file_path, 'r', newline='') as original, open(temp_file_path, 'w', newline='') as temp: 
        reader = csv.reader(original)
        writer = csv.writer(temp)
        
        # Aunque esto sea O(n^2), al ser tan acotados los archivos, no es tan costoso.
        for line in reader: 
            for record in records:
                if line[UNIQUE_IDENTIFIER_INDEX] == record[UNIQUE_IDENTIFIER_INDEX]:
                    found_duplicate = True
                    logging.debug(f'[Atomic append] Found duplicate, discarding: {records}')
                    break
            if found_duplicate: break
            writer.writerow(line)

        for record in records:
            writer.writerow(record)

    if found_duplicate:
        os.remove(temp_file_path)
    else:
        os.replace(temp_file_path, file_path)


def _write_batch_by_range(
        dir: str, 
        range_for_partition: int, 
        records: list[list[str]],
        key_index: int
    ):

    os.makedirs(dir, exist_ok=True)
    # get the file for each record in the batch -> {"file_name": [record1, record2], ....}
    records_per_file = group_by_file(range_for_partition, records, key_index)
    # {
    #   'partition_75290.csv': [['35199', '752900', 'Prehistoric Hunt'], ...], 
    #   'partition_118045.csv': [['35232', '1180450', 'Exitium'], ...],
    # }
    # logging.debug(f"Records per file: {records_per_file}")
    for file_name, records in records_per_file.items():
        # Files are reduced, this operation is not that costly, but
        # guarantees file integrity
        atomically_append_to_file(dir, file_name, records)


def group_by_file(
    range: int, 
    records: list[list[str]],
    key_index: int = 1
) -> dict[str, list[str]]:
    records_per_file = {}
    FILE_PREFIX = 'partition'
    for record in records:
        try:
            key = int(record[key_index])
            file_name = f"{FILE_PREFIX}_{key//range}.csv"
            records_per_file[file_name] = records_per_file.get(file_name, [])
            records_per_file[file_name].append(record)
        except ValueError as e:
            print(f"Received {key}, Expected a numerical type in its place")
            raise e

    return records_per_file


# TODO: Armarse primero el diccionario de adentro y dsps hacer dict[file_name] = dict
def _group_records(records: dict[str, List[str]]) -> Dict[str, List[str]]:
    FILE_NAME = "platform_count.csv"

    records_per_file = {}
    for record_id, msg_id_list in records.items():
        records_per_file[FILE_NAME] = records_per_file.get(FILE_NAME, [])
        records_per_file[FILE_NAME].append([record_id, msg_id_list])

    return records_per_file


# TODO: Sacar codigo repetido de aca con sum_platform_batch_to_records_per_client
def sum_batch_to_records_per_client(
    dir: str,
    new_records_per_client: dict[str, dict[str, int]],
    logger,
    range_for_partition: int = -1,
    save_first_msg_id: bool = False,
):

    need_to_partition_by_range = range_for_partition != -1
    for client_id, new_records in new_records_per_client.items():
        logging.debug(f"NEW RECORDS: {new_records}")

        client_dir = os.path.join(dir, client_id)

        if need_to_partition_by_range:
            records_per_file = _group_by_file_dict(range_for_partition, new_records)
            logging.debug(f"RECORDS PER FILE (partition): {records_per_file}")
        else:
            records_per_file = _group_records(new_records)
            logging.debug(f"RECORDS PER FILE (not partitioned): {records_per_file}")

        sum_batch_to_records(
            client_dir, records_per_file, logger, save_first_msg_id=save_first_msg_id
        )


def _group_by_file_dict(
    range_for_partition: int, records: dict[str, int]
) -> dict[str, list[str]]:
    FILE_PREFIX = "partition"

    records_per_file = {}
    for record_id, value in records.items():
        try:
            key = int(record_id)
            file_name = f"{FILE_PREFIX}_{key//range_for_partition}.csv"
            records_per_file[file_name] = records_per_file.get(file_name, [])
            records_per_file[file_name].append([record_id, value])
        except ValueError as e:
            print(f"Received {key}, Expected a numerical type in its place")
            raise e

    return records_per_file


def create_file_if_unexistent(full_path: str):
    if not os.path.exists(full_path):
        open(full_path, "w").close()


def sum_batch_to_records(
    dir: str,
    records_per_file: dict[str, list[(str, int)]],
    logger,
    save_first_msg_id: bool = False,
):

    os.makedirs(dir, exist_ok=True)
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
                if save_first_msg_id:
                    read_msg_id = row[1]
                    read_record_value = int(row[2])
                else:
                    read_record_value = int(row[1])

                for i, record in enumerate(records):
                    # cada record es: [WINDOWS, MSG_ID_LIST]
                    # key = int(record[0]) #app_id
                    key = record[0]
                    msg_ids = record[1]
                    msg_ids_used_in_file.extend(msg_ids)
                    if read_record_key == key:
                        if save_first_msg_id:
                            new_state = [
                                read_record_key,
                                read_msg_id,
                                str(read_record_value + len(msg_ids)),
                            ]
                        else:
                            new_state = [
                                read_record_key,
                                str(read_record_value + len(msg_ids)),
                            ]
                        writer.writerow(new_state)
                        new_file_lines.append(",".join(new_state))
                        record_was_updated = True
                        records.pop(i)
                        break

                if not record_was_updated:
                    writer.writerow(row)
                    new_file_lines.append(",".join(row))

            for record in records:
                key = record[0]
                msg_ids = record[1]
                if save_first_msg_id:
                    line = [key, msg_ids[0], str(len(msg_ids))]
                else:
                    line = [key, str(len(msg_ids))]

                msg_ids_used_in_file.extend(msg_ids)
                writer.writerow(line)
                new_file_lines.append(",".join(line))

        # TODO: esto lo deberia recibir por parametro y el path se deberia armar aca...
        logging.debug(f"MSG IDS USED IN FILE: {msg_ids_used_in_file}")
        client_id = dir.rsplit("/", maxsplit=1)[-1]
        logger.log(client_id, [file_path] + new_file_lines, msg_ids_used_in_file)

        os.replace(temp_file, file_path)


# Esta la usa el TOP K
def add_batch_to_sorted_file_per_client(
    dir: str,
    records: List[List[str]],
    ascending: bool = True,
    limit: int = float("inf"),
):
    # NEW_RECORDS = {
    # client_id: [
    #       [msg_id, name, avg_playtime_forever],
    #       [...], ...,
    # ...}
    records_per_client = group_batch_by_field(records)
    logging.debug(f"NEW RECORDS PER CLIENT: {records_per_client}")
    for client_id, batch in records_per_client.items():
        client_dir = os.path.join(dir, client_id)

        _add_batch_to_sorted_file(
            client_dir, 
            batch, 
            ascending=ascending, 
            limit=limit
        )


# JUST FOR _add_batch_to_sorted_file use
def _remove_duplicate_msg_ids_from_records(
    sorted_records: list[list[str]], read_msg_id: str
):
    for record in sorted_records:

        if read_msg_id in record[2]:
            sorted_records.remove(record)


def _add_batch_to_sorted_file(
    dir: str,
    new_records: List[List[str]],
    ascending: bool = True,
    limit: int = float("inf"),
):
    """
    Given a series of new_records that has the following format:

        [
            [UNIQUE_IDENTIFIER, NAME, FIELD_TO_SORT_BY]
            , ...
        ]

    it processed the batch and adds it onto a sorted file.

    Duplicates of UNIQUE_IDENTIFIER are not allowed and will be discarded.
    """
    # Por que hay un msg id para la recuperacion?:
    # si tengo varios juegos con el mismo nombre, y justo me coinciden en el count, y justo entran en el top,
    # entonces un top con repetidos seria valido, por lo tanto no puedo filtrar por igualdad de name,count

    if limit <= 0:
        logging.error(f"Error, K must be > 0. Got: {limit}")
        return
    if ascending:
        sorting_key = lambda x: (int(x[1]), x[0])
    else:
        sorting_key = lambda x: (-int(x[1]), x[0])

    # File creation
    file_path = os.path.join(dir, f"sorted_file.csv")
    os.makedirs(dir, exist_ok=True)

    create_file_if_unexistent(file_path)

    # Records are ordered as needed
    # NAME, VALUE, MSG_ID
    new_records = [[record[1], record[2], record[0]] for record in new_records]
    logging.debug(f"Mapped new records: {new_records}")
    sorted_records = sorted(new_records, key=sorting_key)

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

            read_name, read_value, read_msg_id = line
            read_value = int(read_value)

            if not ascending:
                read_value = -read_value

            _remove_duplicate_msg_ids_from_records(sorted_records, read_msg_id)

            for i, new_record in enumerate(sorted_records):
                if amount_of_records_in_top == limit:
                    break

                new_record_name, new_record_value, new_record_msg_id = new_record
                new_record_value = int(new_record_value)

                if not ascending:
                    new_record_value = -new_record_value

                amount_of_records_in_top += 1

                # Extra parameter
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

    # 1 - Esto no se completo
    # 2 - Esto se completo
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
