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
MIN_MEMORY_SPACE = 1024 # 1KB
WAIT_BETWEEN_TRIES = 5
MAX_RETRIES = 3

class WatchdogClient:

    def __init__(self, monitors_ip, monitor_port, client_name:str, client_middleware: Middleware):
        self._stop = False
        self._monitors_ip = monitors_ip
        self._monitor_port = monitor_port
        self._client_name = client_name
        self._client_middleware = client_middleware
        self._connection = None

    def start(self):

        while not self._stop:
            try:
                self._connection = self._conect_to_monitor()

                if not self._connection:
                    logging.error("Could not connect to any monitor")
                    return
                
                logging.info(f"[MONITOR] Connected to monitor. Sending name: {self._client_name}")
                self._connection.send(self._client_name)#envio el nombre

                self._answer_heartbeats()

            except (OSError, TimeoutError)as e:
                sleep(0.5) # so it does not mistake sigterm with closed socket
                if not self._stop:
                    logging.debug("[MONITOR]: monitor is down. waiting for reconnection")
            finally:
                if self._connection:
                    self._connection.close()

    def stop(self):
        self._stop = True
        #self._connection.close()


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
        
        
    def _conect_to_monitor(self):

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        for monitor_ip in reversed(self._monitors_ip):
            
            for j in range(1, MAX_RETRIES + 1):
                if self._stop: return 
                try:
                    client_socket.connect((monitor_ip,  self._monitor_port))
                    return ClientConnection(client_socket, None)
                
                except OSError as _:
                    if j == MAX_RETRIES:
                        logging.info(f"Couldn't connect to monitor {monitor_ip}")
                    sleep(WAIT_BETWEEN_TRIES)

    
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
