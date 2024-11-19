import socket
from .client_connection import ClientConnection

class ServerSocket:

    def __init__(self, port: int):
        # Initialize server socket
        self._port = port
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        #so it can use the port when program ends abruptly
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def bind(self):
        self._server_socket.bind(('', self._port))
        self._server_socket.listen(5)

    def accept_connection(self) -> ClientConnection:

        conn, _ = self._server_socket.accept()
        # if connection_timeout:
        #     conn.settimeout(connection_timeout)
        return ClientConnection(conn)
    
    def settimeout(self, timeout):
        self._server_socket.settimeout(timeout)
    
    def close(self):
        self._server_socket.close()
    
