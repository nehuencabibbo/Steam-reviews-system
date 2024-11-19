from common.server_socket.server_socket import ServerSocket
from common.server_socket.client_connection import ClientConnection
from node_handler import NodeHandler
from common.leader_election.leader_election import LeaderElection


import multiprocessing
import signal
import logging
import threading
import socket
import subprocess
from time import sleep

WAIT_FOR_CONNECTIONS = 5
NUM_OF_RETRIES = 3
TIMEOUT_BEFORE_FALLEN_CHECK = 60

class Watchdog:
    def __init__(self, server_socket: ServerSocket, config: dict):

        self._node_id = config["NODE_ID"]
        self._wait_between_heartbeats = config["WAIT_BETWEEN_HEARTBEAT"]
        self._election_port = config["ELECTION_PORT"]
        self._leader_comunicaton_port = config["LEADER_COMUNICATION_PORT"]

        self._server_socket = server_socket
        self._peers_socket = ServerSocket(self._leader_comunicaton_port)

        self._nodes = {}
        self._got_sigterm = multiprocessing.Event()

        self._peers_lock = threading.Lock()
        self._peers: list[ClientConnection] = []

        self._leader_election = LeaderElection(self._node_id, self._election_port)
        signal.signal(signal.SIGTERM, self._sigterm_handler)

    #TODO: I HAVE TOO MUCH TRY EXCEPT
    def start(self):
  
        while not self._got_sigterm.is_set():

            try:
                self._leader_election.start_leader_election()
            except OSError as e:
                if not self._got_sigterm.is_set():
                    logging.error(f"GOT ERROR WHILE STARTING ELECTION: {e}")
                return
            
            if self._leader_election.i_am_leader():
                if self._got_sigterm.is_set(): return
                logging.info("I'm the leader")

                thread = threading.Thread(target=self._listen_for_peers, daemon=True)
                thread.start()

                try:
                    self._server_socket.bind()
                except OSError as e:
                    if not self._got_sigterm.is_set():
                        logging.error(f"GOT ERROR WHILE LISTENING TO PEERS: {e}")
                    return

                self._monitor_nodes()

                thread.join()
            else:
                if self._got_sigterm.is_set(): return

                if self._leader_election.get_leader_id() is None:
                    continue

                logging.info("I'm not the leader")
                self._listen_to_leader()


    def _monitor_nodes(self):

        #To restart nodes that did not reconnect
        self._server_socket.settimeout(TIMEOUT_BEFORE_FALLEN_CHECK)
        while not self._got_sigterm.is_set():
            try:
            
                conn = self._server_socket.accept_connection()
            
                node_name = conn.recv()

                self._send_registration_to_peers(node_name)
                #TODO: send registration confirmation message?

                logging.info(f"Node {node_name} connected.")

                handler = NodeHandler(conn, node_name, self._got_sigterm, self._wait_between_heartbeats)
                thread = multiprocessing.Process(target=handler.start, daemon=True)
                thread.start()
                
                if node_name in self._nodes and self._nodes[node_name] is not None:
                    self._nodes[node_name].join()

                self._nodes[node_name] = thread
            except socket.timeout:
                self._reconnect_fallen_nodes()
                continue

            except (OSError, ConnectionError) as e:
                if not self._got_sigterm.is_set():
                    logging.error(f"ERROR: {e}")
                    break
            
        self._release_threads()
        

    def _listen_to_leader(self):
       
        leader_id = self._leader_election.get_leader_id()

        leader_connection = self._connect_to_leader(leader_id) 
        logging.info("Connected to leader")
        while not self._got_sigterm.is_set():
            try:
                node_name = leader_connection.recv()
                logging.info(f"Leader sent a message: {node_name}")
                # i register it, but it has no thread since child does not monitor
                self._nodes[node_name] = None 
            except (OSError, ConnectionError):
                if not self._got_sigterm.is_set():
                    logging.info("The leader is down")
                return
    

    def _listen_for_peers(self):

        try:
            self._peers_socket.bind() 
            self._peers_socket.settimeout(5)
        except (OSError, ConnectionError) as e:
            if not self._got_sigterm.is_set():
                logging.error(f"GOT ERROR WHILE LISTENING TO PEERS: {e}")
            return
        
        while not self._got_sigterm.is_set():
            try:
                child = self._peers_socket.accept_connection()
                logging.info("A child conected :D")
                with self._peers_lock:
                    self._peers.append(child)
            except socket.timeout:
                continue
            except (OSError, ConnectionError) as _:
                return


    def _connect_to_leader(self, leader_id):
        leader_ip = f"watchdog_{leader_id}"
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        for i in range(NUM_OF_RETRIES):
            try:
                client_socket.connect((leader_ip, self._leader_comunicaton_port))
                return ClientConnection(client_socket)
            except OSError as e:
                if i == NUM_OF_RETRIES:
                    raise e
                logging.info("Leader is still preparing. Retrying..")
                sleep(3)

    
    def _send_registration_to_peers(self, node_name):
        logging.info("Sending registration to peers")
        #TODO: send in order from higher id to lower id
        with self._peers_lock:
            for i in range(0, len(self._peers)):
                try:
                    self._peers[i].send(node_name) 
                except (OSError, ConnectionError) as _:
                    if self._got_sigterm: return
                    self._peers.pop(i) # peer was down

    def _reconnect_fallen_nodes(self):
        for node, conn in self._nodes.items():
            if not conn:
                logging.info(f"Node {node} never reconnected. Starting it...")
                subprocess.run(['docker', 'stop', node], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                subprocess.run(['docker', 'start', node], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def _release_threads(self):

        for thread in self._nodes.values():
            if thread is not None:
                thread.join()
      

    def _sigterm_handler(self, signal, frame):
        self._got_sigterm.set()
        self._server_socket.close()
        self._leader_election.stop()
        self._peers_socket.close()

        with self._peers_lock:
            for peer in self._peers:
                peer.close()
        
