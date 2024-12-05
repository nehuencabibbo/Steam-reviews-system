import subprocess
import logging
from common.server_socket.client_connection import ClientConnection
from threading import Event, Lock
class AbnormalNodeStatus(Exception):
    pass

HEARTBEAT_MESSAGE = "A"
NACK_MESSAGE = "N"
RECV_TIMEOUT = 10
RECONNECTION_TIMEOUT = 15

class NodeHandler:

    def __init__(self, node_conn: ClientConnection, node_name: str, got_sigterm: Event, wait_between_heartbeats: float):
        self._node_conn_lock =  Lock()
        self._node_conn = node_conn
        self._node_conn.settimeout(RECV_TIMEOUT)
        self._node_name = node_name
        self._got_sigterm = got_sigterm #Event
        self._wait_between_heartbeats = wait_between_heartbeats
        self._got_new_connection = Event()
        
    def start(self):
        while not self._got_sigterm.is_set():
            try:
            
                logging.debug(f"Sending heartbeat to {self._node_name}")
                with self._node_conn_lock:
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
                    self._handle_node_disconnection()

        if self._node_conn:
            self._node_conn.close()
                    

    def set_new_connection(self, connection: ClientConnection):
        with self._node_conn_lock:
            self._node_conn = connection
        self._got_new_connection.set()


    def _restart_node(self):
        subprocess.run(['docker', 'stop', self._node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(['docker', 'start', self._node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def _handle_node_disconnection(self):
        self._node_conn.close()
        self._node_conn = None

        self._reconnect_node()


    def _reconnect_node(self):
        while not self._got_sigterm.is_set():
            self._restart_node()

            self._got_new_connection.wait(RECONNECTION_TIMEOUT) 

            with self._node_conn_lock:
                if self._node_conn is not None:
                    self._got_new_connection.clear()
                    break
                
            logging.info(f"Node {self._node_name} did not reconnect. Restarting it...")


    def close(self):
        self._got_new_connection.set()
