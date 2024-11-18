import socket
from .client_connection import ClientConnection

class ServerSocket:

    def __init__(self, port: int):
        # Initialize server socket
        self._port = port
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def bind(self):
        self._server_socket.bind(('', self._port))
        self._server_socket.listen(5)

    def accept_connection(self, connection_timeout=None) -> ClientConnection:

        conn, _ = self._server_socket.accept()
        if connection_timeout:
            conn.settimeout(connection_timeout)
        return ClientConnection(conn, connection_timeout)
    
    def settimeout(self, timeout):
        self._server_socket.settimeout(timeout)
    
    def close(self):
        self._server_socket.close()
    
