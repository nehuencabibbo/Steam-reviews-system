import subprocess
import logging
from common.server_socket.client_connection import ClientConnection
from multiprocessing import Event, Lock, Manager

class AbnormalNodeStatus(Exception):
    pass

HEARTBEAT_MESSAGE = "A"
NACK_MESSAGE = "N"
RECV_TIMEOUT = 5
RECONNECTION_TIMEOUT = 15

class NodeHandler:

    def __init__(self, node_conn: ClientConnection, node_name: str, wait_between_heartbeats: float):
        
        self._manager = Manager()
        self._node_conn_lock = Lock()
        self._node_conn = self._manager.dict() #use dict to share class ClientConnection
        self._node_conn["conn"] = node_conn 
        self._node_name = node_name
        self._got_sigterm = Event()
        self._wait_between_heartbeats = wait_between_heartbeats
        self._got_new_connection = Event()

        
    def start(self):
        while not self._got_sigterm.is_set():
            try:
            
                logging.debug(f"Sending heartbeat to {self._node_name}")
                with self._node_conn_lock:
                    self._node_conn["conn"].send(HEARTBEAT_MESSAGE)
                    response = self._node_conn["conn"].recv()

                if response == NACK_MESSAGE:
                    logging.info(f"Abnormal status detected on node {self._node_name}")
                    raise AbnormalNodeStatus
                
                logging.debug(f"Got response from {self._node_name}, sleeping")
                self._got_sigterm.wait(self._wait_between_heartbeats) 

            except (OSError, TimeoutError, AbnormalNodeStatus) as _:
                if not self._got_sigterm.is_set():
                    logging.info(f"Node {self._node_name} is down. Restarting it...")
                    self._handle_node_disconnection()

        if self._node_conn["conn"]:
            self._node_conn["conn"].close()
                    

    def set_new_connection(self, connection: ClientConnection):
        with self._node_conn_lock:
            self._node_conn["conn"] = connection
        self._got_new_connection.set()


    def _restart_node(self):
        subprocess.run(['docker', 'stop', self._node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(['docker', 'start', self._node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def _handle_node_disconnection(self):
        self._node_conn["conn"].close()
        self._node_conn["conn"] = None

        self._reconnect_node()


    def _reconnect_node(self):
        while not self._got_sigterm.is_set():
            self._restart_node()

            self._got_new_connection.wait(RECONNECTION_TIMEOUT) 

            with self._node_conn_lock:
                if self._node_conn["conn"] is not None:
                    self._got_new_connection.clear()
                    break
                
            logging.info(f"Node {self._node_name} did not reconnect. Restarting it...")
        
        logging.info(f"The connection with node {self._node_name} was re-established")


    def close(self):
        self._got_sigterm.set()
        self._got_new_connection.set()
