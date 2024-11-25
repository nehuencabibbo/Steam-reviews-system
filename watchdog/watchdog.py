from common.server_socket.server_socket import ServerSocket
from node_handler import NodeHandler
from common.leader_election.leader_election import LeaderElection
from common.udpsocket.udp_socket import UDPSocket
from leader_discovery_service import LeaderDiscoveryService

import signal
import logging
import threading
import socket #for error handling.
import subprocess
import json
import time

NUMBER_OF_RETRIES = 5
WAIT_FOR_LEADER_TO_SETTLE = 3
TIMEOUT_BEFORE_FALLEN_CHECK = 60
REGISTRATION_CONFIRM = "K"
MAX_TIMEOUT = 10
TIME_BETWEEN_HEARTBETS = 3
HEARTBEAT_MESSAGE = "A"
WATCHDOG_NAME_PREFIX = "watchdog_"

class Watchdog:
    def __init__(self, server_socket: ServerSocket, config: dict):

        self._node_id = config["NODE_ID"]
        self._wait_between_heartbeats = config["WAIT_BETWEEN_HEARTBEAT"]
        self._election_port = config["ELECTION_PORT"]
        self._leader_comunicaton_port = config["LEADER_COMUNICATION_PORT"]

        self._leader_discovery_port = config["LEADER_DISCOVERY_PORT"]

        self._server_socket = server_socket
        self._amount_of_monitors = config["AMOUNT_OF_MONITORS"]

        self._got_sigterm = threading.Event()

        self._leader_election = LeaderElection(self._node_id, self._election_port, self._amount_of_monitors)
        self._leader_discovery = LeaderDiscoveryService(self._leader_discovery_port, self._leader_election)
        self._discovery_thread = threading.Thread(target=self._leader_discovery.run, daemon=True)
        self._discovery_thread.start()

        signal.signal(signal.SIGTERM, self._sigterm_handler)
        signal.signal(signal.SIGINT, self._sigint_handler)

        self._watchdog_setup()


    def _watchdog_setup(self):
        
        with open("names/node_names.json", "r") as file:
            nodes_names = json.load(file)
            self._nodes = {key: None for key in nodes_names}
        
        with open("names/monitor_names.json", "r") as file:
            peer_names = json.load(file)
            self._peers = {key: None for key in peer_names if key != f"{WATCHDOG_NAME_PREFIX}{self._node_id}"}
           

    def start(self):
        logging.info("Starting watchdog")

        self._leader_election.look_for_leader()
  
        while not self._got_sigterm.is_set():

            if self._leader_election.get_leader_id() is None:
                self._leader_election.start_leader_election()

            if self._leader_election.i_am_leader():
                if self._got_sigterm.is_set(): return
                logging.info("I'm the leader")
                self._monitor_nodes()

            else:
                if self._got_sigterm.is_set(): return
                # There was no leader. It can happen when the node with greater ID fails during the election
                if self._leader_election.get_leader_id() is None:
                    continue

                logging.info("I'm not the leader")
                self._listen_to_leader()

            
    def _monitor_nodes(self):

        thread = threading.Thread(target=self._listen_for_peers, daemon=True)
        thread.start()

        try:
            self._server_socket.bind()
        except OSError as e:
            if not self._got_sigterm.is_set():
                logging.error(f"[LEADER] GOT ERROR WHILE BINDING PORT: {e}")
            return

        self._server_socket.settimeout(TIMEOUT_BEFORE_FALLEN_CHECK)
        while not self._got_sigterm.is_set():
            try:
            
                conn = self._server_socket.accept_connection()
                node_name = conn.recv()
                logging.info(f"[LEADER] Node {node_name} connected.")

                handler = NodeHandler(conn, node_name, self._got_sigterm, self._wait_between_heartbeats)
                node_thread = threading.Thread(target=handler.start, daemon=True)
                node_thread.start()

                conn.send(REGISTRATION_CONFIRM)
                
                if node_name in self._nodes and self._nodes[node_name] is not None:
                    self._nodes[node_name].join()

                self._nodes[node_name] = node_thread
            except socket.timeout:
                self._reconnect_fallen_nodes()
                continue
            except (OSError, ConnectionError) as e:
                if not self._got_sigterm.is_set():
                    logging.error(f"ERROR: {e}")
                    break
            
        self._release_threads()
        thread.join()
        

    def _listen_to_leader(self):

        leader_id = self._leader_election.get_leader_id()
        leader_addr = (f"{WATCHDOG_NAME_PREFIX}{leader_id}", self._leader_comunicaton_port)

        leader_socket = UDPSocket(amount_of_retries=NUMBER_OF_RETRIES)
        
        time.sleep(WAIT_FOR_LEADER_TO_SETTLE)

        while not self._got_sigterm.is_set():
            try:
                message = f"{HEARTBEAT_MESSAGE},{WATCHDOG_NAME_PREFIX}{self._node_id}"
                leader_socket.send_message(message, leader_addr)
                self._got_sigterm.wait(TIME_BETWEEN_HEARTBETS)

            except (OSError, ConnectionError, socket.timeout) as _:
                if not self._got_sigterm.is_set():
                    logging.info("The monitor is down")
                    self._leader_election.set_leader_death()
                break
        
        leader_socket.close()


    def _listen_for_peers(self):

        leader_socket = UDPSocket() 
        leader_socket.bind(("", self._leader_comunicaton_port))
        leader_socket.settimeout(timeout=2)

        #set start_time to check if a node never connected
        for peer_name in self._peers.keys():
            self._peers[peer_name] = time.time()

        while not self._got_sigterm.is_set():
            try:
                msg: str = leader_socket.recv_message(amount_of_bytes=10)
                _, node_name = msg.split(",")
                logging.debug(f"NODE {node_name} SENT HEARTBEAT")
                self._peers[node_name] = time.time()

            except socket.timeout:
                self._check_peer_status() 
            except (OSError, ConnectionError) as e:
                if not self._got_sigterm.is_set():
                    logging.error(f"Connection closed {e}") 


    def _check_peer_status(self):
        current_time = time.time()
        for peer_name, last_heartbeat in self._peers.items():
            if  current_time - last_heartbeat > MAX_TIMEOUT: 
                self._restart_fallen_node(peer_name)
                self._peers[peer_name] = time.time()       


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

    def _sigint_handler(self, signal, frame):
        #so the killed process does not participate on the next election
        self._leader_election.stop()