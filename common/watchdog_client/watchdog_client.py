import socket
import logging
from server_socket.client_connection import ClientConnection

TIMEOUT = 3
class WatchdogClient:

    def __init__(self, monitor_ip, monitor_port, client_name:str):
        self._stop = False
        self._monitor_ip = monitor_ip
        self._monitor_port = monitor_port
        self._client_name = client_name

    def start(self):

        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self._monitor_ip,  self._monitor_port))

            connection = ClientConnection(client_socket, TIMEOUT)

            connection.send(self._client_name)#envio el nombre

            while not self._stop: #close connection when i got the sigterm?
                msg = connection.recv() # if timeout, raises error
                connection.send(msg)

        except (OSError, TimeoutError)as e:
            if not self._stop:
                #for now if the monitor is down, is an error. This can be fixed wit leader election
                logging.error(f"Error while listening to monitor. {e}")
        finally:
            connection.close()

    def stop(self):
        self._stop = True