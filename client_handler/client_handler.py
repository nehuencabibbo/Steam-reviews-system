import logging
from typing import *
import uuid

from common.client_middleware.client_middleware import ClientMiddleware
from common.middleware.middleware import Middleware, MiddlewareError
from common.protocol.protocol import Protocol
from common.storage import storage

END_TRANSMISSION_MESSAGE = "END"
APP_ID = 0


class ClientHandler:

    def __init__(
        self, client_middleware: ClientMiddleware, middleware: Middleware, **kwargs
    ):
        self._middleware = middleware
        self._client_middleware = client_middleware
        self._got_sigterm = False
        self._ends_received = 0
        self._port = kwargs["CLIENTS_PORT"]
        self._new_clients_exchange_name = kwargs["NEW_CLIENTS_EXCHANGE_NAME"]
        # signal.signal(signal.SIGTERM, self.__sigterm_handler)

    def run(self):
        # No input queues are created as we will hear connections from zmq socket
        # self._middleware.create_queue(self._config["FORWARDING_QUEUE_NAME"])
        self._client_middleware.create_socket("REP")
        self._client_middleware.bind(self._port)
        # socket = self._client_middleware.create_and_bind_socket(self._port)
        while True:
            _ = self._client_middleware.recv_string()
            logging.debug(f"Received new session request")
            session_id = f"{uuid.uuid4()}"
            self._client_middleware.send_string(session_id)
            self._middleware.publish_message(
                [session_id],
                exchange_name=self._new_clients_exchange_name,
            )
