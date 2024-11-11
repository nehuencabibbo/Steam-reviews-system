import socket
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from server_socket.client_connection import ClientConnection

class WatchdogClient:

    def __init__(self, monitor_ip, monitor_port, client_name:str):
        self._stop = False
        self._monitor_ip = monitor_ip
        self._monitor_port = monitor_port
        self._client_name = client_name

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self._monitor_ip,  self._monitor_port))
        self._connection = ClientConnection(client_socket, None)

    def start(self):

        try:
            logging.info(f"[MONITOR] Sending name: {self._client_name}")
            self._connection.send(self._client_name)#envio el nombre

            while not self._stop: 
                logging.debug("[MONITOR] Waiting for heartbeat")
                msg = self._connection.recv()
                logging.debug("[MONITOR] Got heartbeat")
                self._connection.send(msg)

        except (OSError, TimeoutError)as e:
            if not self._stop:
                #for now if the monitor is down, is an error. This can be fixed wit leader election
                logging.error(f"Error while listening to monitor. {e}")
        finally:
            self._connection.close()

    def stop(self):
        self._stop = True
        self._connection.close()
        
