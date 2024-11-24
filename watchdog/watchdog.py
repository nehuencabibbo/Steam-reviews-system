from common.server_socket.server_socket import ServerSocket
from common.server_socket.client_connection import ClientConnection
from node_handler import NodeHandler
from common.leader_election.leader_election import LeaderElection
from common.udpsocket.udp_socket import UDPSocket
from leader_discovery_service import LeaderDiscoveryService

import signal
import logging
import threading
import socket #for error handling. TODO: custom errors for socket
import subprocess
import json
import time

WAIT_FOR_CONNECTIONS = 5
NUM_OF_RETRIES = 3
TIMEOUT_BEFORE_FALLEN_CHECK = 60
REGISTRATION_CONFIRM = "K"
MAX_TIMEOUT = 10

class Watchdog:
    def __init__(self, server_socket: ServerSocket, config: dict):

        self._node_id = config["NODE_ID"]
        self._wait_between_heartbeats = config["WAIT_BETWEEN_HEARTBEAT"]
        self._election_port = config["ELECTION_PORT"]
        self._leader_comunicaton_port = config["LEADER_COMUNICATION_PORT"]

        self._leader_discovery_port = config["LEADER_DISCOVERY_PORT"]#10015

        self._server_socket = server_socket
        self._amount_of_monitors = config["AMOUNT_OF_MONITORS"]

        self._got_sigterm = threading.Event()

        self._leader_election = LeaderElection(self._node_id, self._election_port, self._amount_of_monitors)
        
        self._leader_discovery = LeaderDiscoveryService(self._leader_discovery_port, self._leader_election)
        self._discovery_thread = threading.Thread(target=self._leader_discovery.run, daemon=True)
        self._discovery_thread.start()

        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self._watchdog_setup()


    def _watchdog_setup(self):
        
        with open("names/node_names.json", "r") as file:
            nodes_names = json.load(file)
            self._nodes = {key: None for key in nodes_names}
        
        with open("names/monitor_names.json", "r") as file:
            peer_names = json.load(file)
            self._peers = {key: None for key in peer_names if key != f"watchdog_{self._node_id}"}
           

    #TODO: I HAVE TOO MUCH TRY EXCEPT
    def start(self):
        
        try:
            self._leader_election.look_for_leader()
        except OSError as e:
            if not self._got_sigterm.is_set():
                logging.error(f"GOT ERROR WHILE LOOKING FOR LEADER: {e}")
            return    
  
        while not self._got_sigterm.is_set():

            try:
                if self._leader_election.get_leader_id() is None:
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
                time.sleep(3) #wait for leader to settle #TODO: search for another way (more retries in udp socket?)
                self._listen_to_leader()

            
    def _monitor_nodes(self):
        self._server_socket.settimeout(TIMEOUT_BEFORE_FALLEN_CHECK)
        while not self._got_sigterm.is_set():
            try:
            
                conn = self._server_socket.accept_connection()
            
                node_name = conn.recv()
                
                logging.info(f"Node {node_name} connected.")

                handler = NodeHandler(conn, node_name, self._got_sigterm, self._wait_between_heartbeats)
                thread = threading.Thread(target=handler.start, daemon=True)
                thread.start()

                #TODO:remove registration? now it is not needed?
                conn.send(REGISTRATION_CONFIRM)
                
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
        leader_addr = (f"watchdog_{leader_id}", self._leader_comunicaton_port)

        leader_socket = UDPSocket(timeout=1, amount_of_retries=5) 

        while not self._got_sigterm.is_set():
            try:
                leader_socket.send_message(f"A,watchdog_{self._node_id}", leader_addr)
                self._got_sigterm.wait(3)
            except (OSError, ConnectionError, socket.timeout) as e:
                if not self._got_sigterm.is_set():
                    logging.info("The monitor is down")
                    self._leader_election.set_leader_death()
                break
        
        leader_socket.close()


    def _listen_for_peers(self):

        leader_socket = UDPSocket() 

        leader_socket.bind(("", self._leader_comunicaton_port))
        leader_socket.settimeout(2)

        #set start_time to check if a node never connected
        for peer_name, last_heartbeat in self._peers.items():
            self._peers[peer_name] = time.time()

        while not self._got_sigterm.is_set():
            try:

                msg: str = leader_socket.recv_message(10)
                _, node_name = msg.split(",")

                logging.debug(f"NODE {node_name} SENT HEARTBEAT")

                self._peers[node_name] = time.time()
            except socket.timeout as e:
                current_time = time.time()
                for peer_name, last_heartbeat in self._peers.items():
                    if  current_time - last_heartbeat > MAX_TIMEOUT: 
                        self._restart_fallen_node(peer_name)
                        self._peers[peer_name] = time.time() 
                
            except (OSError, ConnectionError) as e:
                if not self._got_sigterm.is_set():
                    logging.error(f"Connection closed {e}")        


    def _restart_fallen_node(self, node_name):
        logging.info(f"Node {node_name} never reconnected. Starting it...")
        subprocess.run(['docker', 'stop', node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(['docker', 'start', node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def _reconnect_fallen_nodes(self):
        for node, conn in self._nodes.items():
            if not conn:
                self._restart_fallen_node(node)
                

    def _release_threads(self):

        for thread in self._nodes.values():
            if thread is not None:
                thread.join()
      

    def _sigterm_handler(self, signal, frame):
        self._got_sigterm.set()
        self._server_socket.close()
        self._leader_election.stop()
        self._leader_discovery.close()

