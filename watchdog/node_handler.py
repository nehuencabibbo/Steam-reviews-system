import subprocess
import logging
from threading import Event
from common.server_socket.client_connection import ClientConnection

HEARTBEAT_MESSAGE = "A"
class NodeHandler:

    def __init__(self, node_conn: ClientConnection, node_name: str, got_sigterm: Event):
        self._node_conn = node_conn
        self._node_name = node_name
        self._got_sigterm = got_sigterm 

    def start(self):
        try:
            while not self._got_sigterm.is_set(): #close connection when i got the sigterm?
                self._node_conn.send(HEARTBEAT_MESSAGE)
                self._node_conn.recv() # if timeout, raises error
                self._got_sigterm.wait(30) #time between heartbeats

        except (OSError, TimeoutError) as e:
            if not self._got_sigterm.is_set():
                res = subprocess.run(['docker', 'start', self._node_name],
                                check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                                )
                logging.info('Starting container\n {}\n Error={}'.format(res.stdout, res.stderr))
        finally:
            self._node_conn.close()

        #result = subprocess.run(['docker', 'start', 'docker-from-docker_counter_1'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #logging.info('Starting container\n {}\n Error={}'.format(result.stdout, result.stderr))

