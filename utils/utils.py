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


def group_batch_by_field(records: List[List[str]], field_index_to_group_by: int = 0) -> Dict[str,List[List[str]]]:
    '''
    Given a list of records, it groups based on the first element of it
    and returns the groups.

    Ex:

    records = [['a', '1', '2'], ['b', '1', '2'], ['a', '3', '4']]

    returns:

    {'a': [['1', '2'], ['3', '4']], 'b': [['1', '2']]}
    '''
    batch_per_field = {}

    # Get the batch for every client
    for record in records:
        field_to_group_by = record[field_index_to_group_by]
        # Get record but without field_to_group_by
        record = record[:field_index_to_group_by] + record[field_index_to_group_by + 1:]
        if not field_to_group_by in batch_per_field:
            batch_per_field[field_to_group_by] = []

        batch_per_field[field_to_group_by].append(record)
    return batch_per_field


def group_msg_ids_per_client_by_field(
    body: list[list],
    client_id_index: int,
    msg_id_index: int,
    field_to_group_by: int,
    use_field_to_group_by_in_key: bool = False, 
    #TODO: Cuando el drop nulls mande todo junto en vez de como manda ahora, esto vuela, queda ahora
    # para que ande nomas  
) -> Dict[str, Dict[str, List[str]]]:
    # TODO: Arreglar descripcion despues de volar use_field_to_grup_by_in_key
    '''
    Retorna una lista con los msg ids involucrados para cada mensaje, sacar la cantidad a sumar
    es hacer len(lista) por eso no se guarda el count tambien.

    Se devuelve algo del estilo: 
    {client_id: {Windows: ['1,W', '3,W', '4,W', '5,W'], Linux: ['2,L', '10,L'], Mac: ['9,M']}, ...}

    Explicacion message id: 
    Como un mismo mensaje obtiene si el juego esta soportado para una plataforma o no, pero el drop nulls
    manda todos los mensajes con un mismo msg_id, que esta bien, porque para el es todo un mismo mensaje,
    yo tengo que agregar un identificador unico para cada uno (porque para el count by platform son distintos 
    mensajes), para evitar guardar estado, se guarda msg_id,inicial_platform, asi se diferencian
    '''
    msg_ids_per_record_by_client_id = {}
    for record in body:
        client_id = record[client_id_index]
        msg_id = record[msg_id_index]
        record_id = record[field_to_group_by]

        if not client_id in msg_ids_per_record_by_client_id:
            msg_ids_per_record_by_client_id[client_id] = {}

        if not record_id in msg_ids_per_record_by_client_id[client_id]:
            msg_ids_per_record_by_client_id[client_id][record_id] = []

        if use_field_to_group_by_in_key:
            msg_id = str(ord(record_id[0])) + msg_id
            print('quack', msg_id)
    
        msg_ids_per_record_by_client_id[client_id][record_id].append(msg_id)

    return msg_ids_per_record_by_client_id