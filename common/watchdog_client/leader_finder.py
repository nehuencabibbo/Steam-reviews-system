import socket
import logging
import os, sys
from time import sleep

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from server_socket.client_connection import ClientConnection

WAIT_BETWEEN_TRIES = 7
MAX_RETRIES = 5
WAIT_LEADER_ELECTION_RUNNING = 5
LEADER_ELECTION_RUNNING_MESSAGE = "F"

class LeaderFinder:
    def __init__(self, monitors_ip: list[str], leader_discovery_port: int):
        self._monitors_ip = monitors_ip
        self._leader_discovery_port = leader_discovery_port
        self._stop = False

    def look_for_leader(self):
        logging.debug("Looking for leader")
        for _ in range(MAX_RETRIES):
            sleep(WAIT_BETWEEN_TRIES)
            if self._stop:
                return

            for monitor_ip in self._monitors_ip:
                logging.debug(f"[MONITOR] Trying with monitor {monitor_ip}")
                try:
                    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client_socket.connect((monitor_ip, self._leader_discovery_port))
                    connection = ClientConnection(client_socket)
                    logging.debug(f"Connected to {monitor_ip}")
                    return self._get_leader_id(connection)

                except OSError as _:
                    logging.debug(f"Error connecting to monitor {monitor_ip}")
                    continue
        
        logging.warning("Reached max retries and couldnt connect to the monitor")

        return None
    

    def _get_leader_id(self, connection: ClientConnection):
        
        while not self._stop:
            msg = connection.recv()
            logging.debug(f"Got message: {msg}")

            if msg == LEADER_ELECTION_RUNNING_MESSAGE:
                logging.debug("Leader election in progress, waiting...")
                sleep(WAIT_LEADER_ELECTION_RUNNING) 
            else:
                return msg

    
    def stop(self):
        self._stop = True