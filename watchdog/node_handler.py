import subprocess
import logging
#from threading import Event
from common.server_socket.client_connection import ClientConnection

HEARTBEAT_MESSAGE = "A"
class NodeHandler:

    def __init__(self, node_conn: ClientConnection, node_name: str, got_sigterm, wait_between_heartbeats: float):
        self._node_conn = node_conn
        self._node_name = node_name
        self._got_sigterm = got_sigterm #multiprocessing Event
        self._wait_between_heartbeats = wait_between_heartbeats

    def start(self):
        try:
            while not self._got_sigterm.is_set():
                logging.info(f"Sending heartbeat to {self._node_name}")
                self._node_conn.send(HEARTBEAT_MESSAGE)
                self._node_conn.recv() # if timeout, raises error
                logging.info(f"Got response from {self._node_name}, sleeping")
                self._got_sigterm.wait(self._wait_between_heartbeats) 

        except (OSError, TimeoutError) as _:
            if not self._got_sigterm.is_set():
                logging.info(f"Node {self._node_name} is down.")
                subprocess.run(['docker', 'start', self._node_name],
                                check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                                )
                logging.info(f"Starting container {self._node_name}")
        finally:
            self._node_conn.close()


