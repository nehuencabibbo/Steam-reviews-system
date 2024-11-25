import socket
import logging
import sys
import os
import threading
from shutil import disk_usage
from time import sleep

from .leader_finder import LeaderFinder

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from middleware.middleware import Middleware
from server_socket.client_connection import ClientConnection


NACK_MSG = "N"
REGISTRATION_CONFIRM = "K"
MIN_MEMORY_SPACE = 1024 # 1KB

class WatchdogClient:

    def __init__(self, monitors_ip: list[str], monitor_port: int, client_name: str, discovery_port: int, client_middleware: Middleware):
        self._stop = False
        self._monitor_port = monitor_port
        self._client_name = client_name
        self._client_middleware = client_middleware
        self._connection = None
        self._leader_finder = LeaderFinder(monitors_ip, discovery_port)

    def start(self):

        while not self._stop:
            try:
                logging.debug("[MONITOR] Looking for leader")
                self._connection = self._conect_to_monitor()

                if not self._connection:
                    logging.debug("[MONITOR] Could not connect to any monitor. Retrying...")
                    continue
                
                logging.debug(f"[MONITOR] Connected to monitor. Checking in...")
                self._register()

                self._answer_heartbeats()

            except (OSError, TimeoutError) as e:
                sleep(0.5) # so it does not mistake sigterm with closed socket
                if not self._stop:
                    logging.debug("[MONITOR]: monitor is down. waiting for reconnection")
            finally:
                if self._connection:
                    self._connection.close()


    def _conect_to_monitor(self):
        leader_id = self._leader_finder.look_for_leader()
        logging.debug(f"[MONITOR] The leader is: {leader_id}")

        if leader_id is None:
            return None
        
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        monitor_ip = f"watchdog_{leader_id}"
        try:
            client_socket.connect((monitor_ip,  self._monitor_port))
            return ClientConnection(client_socket)
        
        except OSError as _:
            if not self._stop:
                logging.debug("[MONITOR] Couldnt connect to leader. The leader is down or is preparing")


    def _register(self):
        self._connection.send(self._client_name)
        msg = self._connection.recv()
        if msg == REGISTRATION_CONFIRM:
            logging.debug("[MONITOR] Registration confirmed")


    def stop(self):
        self._stop = True
        self._leader_finder.stop()


    def _answer_heartbeats(self):

        while not self._stop: 

            logging.debug("[MONITOR] Waiting for heartbeat")
            msg = self._connection.recv()

            if not self._check_node_general_functionality():
                logging.debug("System status is abnormal, sending NACK")
                self._connection.send(NACK_MSG)
                return False

            logging.debug("System status is OK, sending ACK")
            self._connection.send(msg)
        

    def _check_node_general_functionality(self):

        # Check free space.
        _, _, free_space = disk_usage(__file__)
        if free_space < MIN_MEMORY_SPACE:
            return False

        # Check if i can write files
        try:
            with open("/tmp/test_write.txt", 'w') as file:
                file.write("hola")
            os.remove("/tmp/test_write.txt")
        except OSError:
            return False
        
        # Check rabbit connection
        if not self._client_middleware.check_connection():
            return False 
        
        # Check if main thread is alive
        return threading.main_thread().is_alive()

