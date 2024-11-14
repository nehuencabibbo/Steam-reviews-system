import subprocess
import logging
#from threading import Event
from common.server_socket.client_connection import ClientConnection

class AbnormalNodeStatus(Exception):
    pass

HEARTBEAT_MESSAGE = "A"
NACK_MESSAGE = "N"
class NodeHandler:

    def __init__(self, node_conn: ClientConnection, node_name: str, got_sigterm, wait_between_heartbeats: float):
        self._node_conn = node_conn
        self._node_name = node_name
        self._got_sigterm = got_sigterm #multiprocessing Event
        self._wait_between_heartbeats = wait_between_heartbeats

    def start(self):
        try:
            while not self._got_sigterm.is_set():
                logging.debug(f"Sending heartbeat to {self._node_name}")

                self._node_conn.send(HEARTBEAT_MESSAGE)
                response = self._node_conn.recv()

                if response == NACK_MESSAGE:
                    # si es un nack, debo levantarlo otra vez
                    logging.info(f"Abnormal status detected on node {self._node_name}")
                    raise AbnormalNodeStatus
                
                
                logging.debug(f"Got response from {self._node_name}, sleeping")
                self._got_sigterm.wait(self._wait_between_heartbeats) 

        except (OSError, TimeoutError, AbnormalNodeStatus) as _:
            if not self._got_sigterm.is_set():
                subprocess.run(['docker', 'stop', self._node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                logging.info(f"Node {self._node_name} is down.")
                subprocess.run(['docker', 'start', self._node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                logging.info(f"Starting container {self._node_name}")
        finally:
            self._node_conn.close()


