import logging
import os, sys
from time import sleep

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from server_socket.tcp_middleware import TCPMiddleware

WAIT_BETWEEN_TRIES = 2
MAX_RETRIES = 10
WAIT_LEADER_ELECTION_RUNNING = 5
LEADER_ELECTION_RUNNING_MESSAGE = "F"

class LeaderFinder:
    def __init__(self, monitors_ip: list[str], leader_discovery_port: int):
        self._monitors_ip = monitors_ip
        self._leader_discovery_port = leader_discovery_port
        self._stop = False
        self._middleware = TCPMiddleware()

    def look_for_leader(self):
        for _ in range(MAX_RETRIES):
            if self._stop:
                return

            for monitor_ip in self._monitors_ip:
                logging.debug(f"[LEADER FINDER] Trying with monitor {monitor_ip}")
                try:    
                    response = self._get_leader_id(monitor_ip)
                    if response is None:
                        break
                    return response

                except (OSError, ConnectionError):
                    logging.debug(f"[LEADER FINDER] Error connecting to monitor {monitor_ip}.")
                    continue
                
            sleep(WAIT_BETWEEN_TRIES) 
        
        logging.debug("[LEADER FINDER] Reached max retries and couldnt connect to the monitor")
        return None
    

    def _get_leader_id(self, monitor_ip: str):

        self._middleware.connect((monitor_ip, self._leader_discovery_port))
        logging.debug(f"[LEADER FINDER] Connected to {monitor_ip}. Asking for leader id")

        msg = self._middleware.receive_message()
        logging.debug(f"[LEADER FINDER] Got message: {msg}")

        if msg == LEADER_ELECTION_RUNNING_MESSAGE:
            logging.debug("[LEADER FINDER] Leader election in progress, waiting before retrying...")
            sleep(WAIT_LEADER_ELECTION_RUNNING) 
            self._middleware.close_connection()
            return
        
        self._middleware.close_connection()
        return msg
            
            
    def stop(self):
        self._stop = True
        self._middleware.close()