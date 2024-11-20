import hashlib
from typing import * 


def node_id_to_send_to(client_id: str, app_id: str, nodes: int) -> int:
    """
    Returns a value between 1 (included) and nodes (included)
    according to the given client_id and app_id
    """
    # return hash(client_id + app_id) % nodes

    hash_input = client_id + app_id
    hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
    return hash_value % nodes


def get_batch_per_client(records: List[List[str]]) -> Dict[str,List[List[str]]]:
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