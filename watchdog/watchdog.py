from common.server_socket.server_socket import ServerSocket
from node_handler import NodeHandler

import multiprocessing
import signal
import logging

class Watchdog:
    def __init__(self, server_socket: ServerSocket, config: dict):
        self._server_socket = server_socket
        self._threads = {}
        self._got_sigterm = multiprocessing.Event()
        self._wait_between_heartbeats = config["WAIT_BETWEEN_HEARTBEAT"]
        signal.signal(signal.SIGTERM, self._sigterm_handler)

    def start(self):
        #TODO: pasar por env todos los nombres y cantidad, asi se que si falta uno luego de n segs (en accept), lo busco y lo levanto
        try:
            while not self._got_sigterm.is_set():

                conn = self._server_socket.accept_connection(5) #set timeout for connection
            
                node_name = conn.recv()

                logging.info(f"Node {node_name} connected.")
                
                handler = NodeHandler(conn, node_name, self._got_sigterm, self._wait_between_heartbeats)
                thread = multiprocessing.Process(target=handler.start, args=())
                thread.start()
                
                if node_name in self._threads:
                    self._threads[node_name].join()

                self._threads[node_name] = thread
                #self._handlers[node_name] = handler
        except (OSError, ConnectionError) as e:
            if not self._got_sigterm.is_set():
                logging.error(f"ERROR: {e}")
        finally:
            [thread.join() for thread in self._threads.values()] #error here
                
    
    def _sigterm_handler(self, signal, frame):
        self._got_sigterm.set()
        self._server_socket.close()
        
