import uuid
import logging

from common.middleware.middleware import Middleware
import zmq


class ClientMiddleware:
    def __init__(self):
        # One I/O thread (for all sockets) is sufficient for all but the most extreme applications. When you
        # create a new context, it starts with one I/O thread. The general rule of thumb is to allow one
        # I/O thread per gigabyte of data in or out per second. To raise the number of I/O threads, use the
        # zmq_ctx_set() call before creating any sockets
        # https://zguide.zeromq.org/docs/chapter2/#High-Level-Messaging-Patterns
        self.__context = zmq.Context(io_threads=1)
        self.__socket = None

    def create_socket(self, sock_type: str):
        self.__socket = self.__context.socket(
            zmq.REP if sock_type == "REP" else zmq.REQ
        )

    def bind(self, port: int):
        self.__socket.bind(f"tcp://*:{port}")

    def connect_to(self, ip: str, port: int):
        self.__socket.connect(f"tcp://{ip}:{port}")

    def send_string(self, message: str):
        self.__socket.send_string(message)

    def recv_string(self):
        return self.__socket.recv_string()
