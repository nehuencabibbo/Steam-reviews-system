import logging
import os, sys
from time import sleep

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from server_socket.tcp_middleware import TCPMiddleware

WAIT_BETWEEN_TRIES = 7
MAX_RETRIES = 5
WAIT_LEADER_ELECTION_RUNNING = 5
LEADER_ELECTION_RUNNING_MESSAGE = "F"

class LeaderFinder:
    def __init__(self, monitors_ip: list[str], leader_discovery_port: int):
        self._monitors_ip = monitors_ip
        self._leader_discovery_port = leader_discovery_port
        self._stop = False
        self._middleware = TCPMiddleware()

    def look_for_leader(self):
        logging.debug("Looking for leader")
        for _ in range(MAX_RETRIES):
            sleep(WAIT_BETWEEN_TRIES)
            if self._stop:
                return

            for monitor_ip in self._monitors_ip:
                logging.debug(f"[MONITOR] Trying with monitor {monitor_ip}")
                try:    
                    return self._get_leader_id(monitor_ip)

                except (OSError, ConnectionError):
                    logging.debug(f"Error connecting to monitor {monitor_ip}")
                    continue
        
        logging.debug("Reached max retries and couldnt connect to the monitor")
        return None
    

    def _get_leader_id(self, monitor_ip: str):

        self._middleware.connect((monitor_ip, self._leader_discovery_port))
        logging.debug(f"Connected to {monitor_ip}")

        while not self._stop:
            msg = self._middleware.receive_message()
            logging.debug(f"Got message: {msg}")

            if msg == LEADER_ELECTION_RUNNING_MESSAGE:
                logging.debug("Leader election in progress, waiting...")
                sleep(WAIT_LEADER_ELECTION_RUNNING) 
            else:
                self._middleware.close_connection()
                return msg
            
            
    def stop(self):
        self._stop = True
        self._middleware.close()