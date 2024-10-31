import threading
import uuid
import logging

from common.middleware.middleware import Middleware
from common.protocol.protocol import Protocol
import zmq

END_TRANSMISSION_MESSAGE = "END"
MAX_POLL_TIME = 2000  # in miliseconds


class ClientMiddleware:
    def __init__(self, batch_size=10, protocol: Protocol = Protocol()):
        # One I/O thread (for all sockets) is sufficient for all but the most extreme applications. When you
        # create a new context, it starts with one I/O thread. The general rule of thumb is to allow one
        # I/O thread per gigabyte of data in or out per second. To raise the number of I/O threads, use the
        # zmq_ctx_set() call before creating any sockets
        # https://zguide.zeromq.org/docs/chapter2/#High-Level-Messaging-Patterns
        self.__context = zmq.Context(io_threads=1)
        self.__socket = None
        self.__batch: list[bytes, int] = [b"", 0]
        self.__batch_size = batch_size
        self.__protocol = protocol

    def create_socket(self, sock_type):
        self.__socket = self.__context.socket(sock_type)

    def bind(self, port: int):
        self.__socket.bind(f"tcp://*:{port}")

    def connect_to(self, ip: str, port: int):
        self.__socket.connect(f"tcp://{ip}:{port}")

    def send_string(self, message: str):
        self.__socket.send_string(message)

    # def recv_string(self):
    #     res = self.__socket.recv_string()
    #     return self.__protocol.decode_batch(res)

    def recv_batch(self):
        res = self.__socket.recv()
        return self.__protocol.decode_batch(res)

    def recv_multipart(self):
        return self.__socket.recv_multipart()

    def send_multipart(self, client_id, message):
        # Send a reply back to the client using its identifier
        logging.debug(f"Sending: {message} to {client_id}")
        self.__socket.send_multipart([client_id, b"", message])

    def send_query_results(self, client_id, message, query):
        message = self.__protocol.add_to_batch(message, [query])
        self.send_multipart(client_id, message)

    def send(self, message: list[str]):
        current_batch, ammount_of_messages = self.__batch
        new_batch = self.__protocol.add_to_batch(current_batch, message)

        if ammount_of_messages + 1 == self.__batch_size:
            self.__socket.send(new_batch)
            self.__batch = [b"", 0]
        else:
            self.__batch = [new_batch, ammount_of_messages + 1]

    def _send_batch(self):
        batch, amount_of_messages = self.__batch

        if amount_of_messages == 0:
            return

        self.__socket.send(batch)
        self.__batch = [b"", 0]

    def send_end(self, end_message: str = END_TRANSMISSION_MESSAGE):
        end_message = self.__protocol.add_to_batch(b"", [end_message])

        self._send_batch()
        self.__socket.send(end_message)

    def register_for_pollin(self):
        self.__poller = zmq.Poller()
        self.__poller.register(self.__socket, zmq.POLLIN)

    def has_message(self):
        socks = dict(self.__poller.poll(MAX_POLL_TIME))
        return self.__socket in socks and socks[self.__socket] == zmq.POLLIN

    # linger is the max time (in miliseconds) waited for all undelivered messages to be sent.
    # https://libzmq.readthedocs.io/en/zeromq3-x/zmq_setsockopt.html (ZMQ_LINGER)
    def shutdown(self, linger=5000):
        self.__context.destroy(linger)
        self.__context.term()
