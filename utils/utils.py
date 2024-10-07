def node_id_to_send_to(client_id: str, app_id: str, nodes: int) -> int:
    """
    Returns a value between 1 (included) and nodes (included)
    according to the given client_id and app_id
    """
    return (hash(client_id + app_id) % nodes) + 1
