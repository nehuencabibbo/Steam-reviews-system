from .udp_socket import UDPSocket
import threading
import socket

class UDPMiddlewareTimeoutError(Exception):
    def __init__(self, message="Socket operation timed out"):
        super().__init__(message)


class UDPMiddleware:

    def __init__(self, send_retries=3):
        self._stop = False

        self._sender_lock = threading.Lock() 
        self._sender_socket = UDPSocket(timeout=0.1, amount_of_retries=send_retries)
        self._receiver_socket = UDPSocket()
        self._addresses_to_broadcast = []


    def bind(self, addr):
        self._receiver_socket.bind(addr)
    

    def receive_message(self, length):
        try:
            return self._receiver_socket.recv_message(length)
        except socket.timeout as _:
            raise UDPMiddlewareTimeoutError


    def broadcast(self, message):
        for addr in self._addresses_to_broadcast:
            if self._stop:
                return

            self.send_message(message, addr)


    def add_addr_to_broadcast(self, addr):
        self._addresses_to_broadcast.append(addr)


    def send_message(self, message, addr):
        with self._sender_lock:
            try:
                return self._sender_socket.send_message(message, addr)
            except (OSError, ConnectionError):
                return False
    

    def set_receiver_timeout(self, timeout):
        self._receiver_socket.settimeout(timeout)
    

    def close(self):
        self._stop = True
        self._receiver_socket.close()
        self._sender_socket.close()
