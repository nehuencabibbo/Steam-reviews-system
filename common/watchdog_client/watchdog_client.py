import socket
import logging
import sys
import os
import threading
from shutil import disk_usage
from time import sleep

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from middleware.middleware import Middleware
from server_socket.client_connection import ClientConnection


NACK_MSG = "N"
REGISTRATION_CONFIRM = "K"
MIN_MEMORY_SPACE = 1024 # 1KB
WAIT_BETWEEN_TRIES = 5
MAX_RETRIES = 5

class WatchdogClient:

    def __init__(self, monitors_ip, monitor_port, client_name:str, discovery_port, client_middleware: Middleware):
        self._stop = False
        self._monitors_ip = monitors_ip
        self._monitor_port = monitor_port
        self._client_name = client_name
        self._client_middleware = client_middleware
        self._connection = None
        self._leader_discovery_port = discovery_port

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

            except (OSError, TimeoutError)as e:
                sleep(0.5) # so it does not mistake sigterm with closed socket
                if not self._stop:
                    logging.debug("[MONITOR]: monitor is down. waiting for reconnection")
            finally:
                if self._connection:
                    self._connection.close()


    def _conect_to_monitor(self):
        leader_id = self._look_for_leader()
        logging.debug(f"[MONITOR] The leader is: {leader_id}")

        if leader_id == None:
            return
        
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        monitor_ip = f"watchdog_{leader_id}"
        try:
            client_socket.connect((monitor_ip,  self._monitor_port))
            return ClientConnection(client_socket)
        
        except OSError as _:
            if not self._stop:
                logging.debug("[MONITOR] Couldnt connect to leader. The leader is down or is preparing")


    def _look_for_leader(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection = None
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

                    connection.send("Hola")

                    while not self._stop:
                        msg = connection.recv()
                        logging.debug(f"Got message: {msg}")

                        if msg == "F":
                            logging.debug("Leader election in progress, waiting...")
                            sleep(5)  # Wait before polling again
                        else:
                            client_socket.close()
                            return msg  # Return the leader ID

                except OSError as e:
                    logging.debug(f"Error connecting to monitor {monitor_ip}")
                    continue
                finally:
                    if connection:
                        connection.close()
        
        logging.warning("Reached max retries and couldnt connect to the monitor")

        return None


    def _register(self):
        self._connection.send(self._client_name)
        msg = self._connection.recv()
        if msg == REGISTRATION_CONFIRM:
            logging.debug("[MONITOR] Registration confirmed")


    def stop(self):
        self._stop = True


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
        

    # TODO: ver que hacer con el chequeo de espacio o de escritura
    def _check_node_general_functionality(self):

        # Check free space. If the free space is lower than 10MB? return false?
        _, _, free_space = disk_usage(__file__)
        if free_space < MIN_MEMORY_SPACE:
            return False

        # chequear que puedo escribir/leer archivos -> chequear memoria disponible?
        # esto o mejor chequeo si quedan mas de x bytes libres?
        try:
            with open("/tmp/test.txt", 'w') as file:
                file.write("hola")
        except OSError:
            return False

        # chequear si anda rabbit
        if not self._client_middleware.check_connection():
            return False 
        
        # chequear si el hilo principal esta vivo -> can change for mp.parent_process().is_alive()
        return threading.main_thread().is_alive()

        # caso aparte, como chequeo si zmq anda? creo que no puedo
