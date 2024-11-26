import socket
from .client_connection import ClientConnection

class TCPMiddlewareTimeoutError(Exception):
    def __init__(self, message="Socket operation timed out"):
        super().__init__(message)


class TCPMiddleware:
    def __init__(self):
        self._socket = None
        self._connection = None
    

    def bind(self, addr, backlog: int = 5):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(addr)
        self._socket.listen(backlog)


    def accept_connection(self) -> tuple[ClientConnection, str]:
        try:
            conn, addr = self._socket.accept()
            return ClientConnection(conn), addr
        
        except socket.timeout:
            raise TCPMiddlewareTimeoutError


    def connect(self, adress):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect(adress)
        self._connection = ClientConnection(self._socket)


    def send_message(self, message: str):
        if self._connection:
            self._connection.send(message)
        

    def receive_message(self) -> str:
        if self._connection:
            return self._connection.recv()
    

    def set_timeout(self, timeout: float):
        self._socket.settimeout(timeout)
    

    def close_connection(self):
        if self._connection:
            self._connection.close()
            self._connection = None


    def close(self):
        if self._socket:
            self._socket.close()

        self.close_connection()

        