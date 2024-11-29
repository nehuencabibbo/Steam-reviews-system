import subprocess
import logging
from common.server_socket.client_connection import ClientConnection

class AbnormalNodeStatus(Exception):
    pass

HEARTBEAT_MESSAGE = "A"
NACK_MESSAGE = "N"
RECV_TIMEOUT = 3

class NodeHandler:

    def __init__(self, node_conn: ClientConnection, node_name: str, got_sigterm, wait_between_heartbeats: float):
        self._node_conn = node_conn
        self._node_conn.settimeout(RECV_TIMEOUT)
        self._node_name = node_name
        self._got_sigterm = got_sigterm #Event
        self._wait_between_heartbeats = wait_between_heartbeats

    def start(self):
        try:
            while not self._got_sigterm.is_set():
                logging.debug(f"Sending heartbeat to {self._node_name}")

                self._node_conn.send(HEARTBEAT_MESSAGE)
                response = self._node_conn.recv()

                if response == NACK_MESSAGE:
                    logging.info(f"Abnormal status detected on node {self._node_name}")
                    raise AbnormalNodeStatus
                
                
                logging.debug(f"Got response from {self._node_name}, sleeping")
                self._got_sigterm.wait(self._wait_between_heartbeats) 

        except (OSError, TimeoutError, AbnormalNodeStatus) as _:
            if not self._got_sigterm.is_set():
                logging.info(f"Node {self._node_name} is down. Restarting it...")
                subprocess.run(['docker', 'stop', self._node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                subprocess.run(['docker', 'start', self._node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        finally:
            self._node_conn.close()


