import socket
from client_connection import ClientConnection

class ServerSocket:

    def __init__(self, port: int):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))

    def accept_connection(self, connection_timeout) -> ClientConnection:

        conn, _ = self._server_socket.accept()
        return ClientConnection.new(conn, connection_timeout)
    
    def close(self):
        self._server_socket.close()
    