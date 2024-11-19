import threading
from time import sleep
from typing import Any, Generator
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
        connection_id, msg = self.__socket.recv_multipart()
        msg_type, msg = Protocol.get_message_type(msg)

        return (
            connection_id,
            msg_type,
            msg,
        )

    def send_multipart(self, connection_id, message: list[str], needs_encoding=False):
        # Send a reply back to the client using its identifier
        logging.debug(f"Sending: {message} to {connection_id}")
        if needs_encoding:
            self.__socket.send_multipart(
                [connection_id, Protocol.add_to_batch(b"", message)]
            )
        else:
            self.__socket.send_multipart([connection_id, b"", message])

    def send_query_results(self, client_id, message: bytes, query: str):
        message = self.__protocol.add_to_batch(message, [query])
        self.send_multipart(client_id, message)

    def send_query_results_from_generator(
        self, client_id, results: Generator[list[str], Any, None], query: str
    ):
        batch = b""
        for res in results:
            batch = self.__protocol.add_to_batch(batch, res)

        message = self.__protocol.add_to_batch(batch, [query])
        logging.info(f"sending multipart to: {client_id}")
        self.send_multipart(client_id, message)

    def send_message(self, message: list[str]):
        batch = self.__protocol.add_to_batch(b"", message)
        logging.info(f"Sending: {batch}")
        self.__socket.send(batch)

    # D: Data
    def send(self, message: list[str], message_type="D", session_id=None):
        current_batch, ammount_of_messages = self.__batch
        new_batch = self.__protocol.add_to_batch(current_batch, message)

        if ammount_of_messages + 1 == self.__batch_size:
            logging.info(f"SEND | Session id: {session_id}")
            message_with_session_id = self.__protocol.insert_before_batch(
                new_batch, [session_id]
            )
            self.__socket.send(
                self.__protocol.insert_before_batch(
                    message_with_session_id, [message_type]
                )
            )
            logging.debug("Sent batch")
            self.__batch = [b"", 0]
        else:
            self.__batch = [new_batch, ammount_of_messages + 1]

    def send_batch(self, message_type=None, session_id=None):
        batch, amount_of_messages = self.__batch

        if amount_of_messages == 0:
            return
        if message_type:
            message_with_session_id = self.__protocol.insert_before_batch(
                batch, [session_id]
            )

            batch = self.__protocol.insert_before_batch(
                message_with_session_id, [message_type]
            )

        self.__socket.send(batch)
        self.__batch = [b"", 0]

    def send_end(self, session_id, end_message: str = [END_TRANSMISSION_MESSAGE]):
        type_message = self.__protocol.add_to_batch(b"", ["D"])
        session_id_message = self.__protocol.add_to_batch(type_message, [session_id])
        end_message = self.__protocol.add_to_batch(session_id_message, end_message)

        self.send_batch(
            message_type="D", session_id=session_id
        )  # In case the is a message, it has to be data
        self.__socket.send(end_message)

    def register_for_pollin(self):
        self.__poller = zmq.Poller()
        self.__poller.register(self.__socket, zmq.POLLIN)

    def has_message(self):
        socks = dict(self.__poller.poll(MAX_POLL_TIME))
        return self.__socket in socks and socks[self.__socket] == zmq.POLLIN

    # def set_session_id(self, session_id):
    #     self.__socket.setsockopt(zmq.IDENTITY, session_id)

    def disconnect(self, ip, port):
        self.__socket.disconnect(f"tcp://{ip}:{port}")

    def close_socket(self, linger=5000):
        self.__socket.close(linger)

    def get_session_id(self, message):
        return self.__protocol.get_session_id(message)

    # linger is the max time (in miliseconds) waited for all undelivered messages to be sent.
    # https://libzmq.readthedocs.io/en/zeromq3-x/zmq_setsockopt.html (ZMQ_LINGER)
    def shutdown(self, linger=5000):
        self.__context.destroy(linger)
        self.__context.term()

    def get_last_message_id(self, batch: bytes) -> int:
        rows_left = batch
        last_row = None
        # get last batch
        while True:
            row, row_length = self.__protocol.get_first_row(rows_left)

            rows_left = rows_left[row_length:]

            if rows_left == b"":
                last_row = row
                break
        # get first field from last batch
        return int(self.__protocol.get_row_field(field=0, encoded_row=last_row))

    def get_row_from_message(self, message: bytes) -> list[str]:
        return self.__protocol.decode(message)
