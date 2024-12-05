import signal
import logging
import threading
import subprocess
import json
import time

from common.server_socket.tcp_middleware import TCPMiddleware, TCPMiddlewareTimeoutError
from common.udpsocket.udp_middleware import UDPMiddleware, UDPMiddlewareTimeoutError
from node_handler import NodeHandler
from common.leader_election.leader_election import LeaderElection
from leader_discovery_service import LeaderDiscoveryService

NUMBER_OF_RETRIES = 5
TIMEOUT_BEFORE_FALLEN_CHECK = 20
REGISTRATION_CONFIRM = "K"
MAX_MONITOR_TIMEOUT = 3
TIME_BETWEEN_HEARTBEATS = 1 #si me envian cada 1 segundo, cada vez que recibo un heartbeat debo revisar si alguno esta caido
HEARTBEAT_MESSAGE = "A"
WATCHDOG_NAME_PREFIX = "watchdog_"

class Watchdog:
    def __init__(self, middleware: TCPMiddleware , config: dict):

        self._node_id = config["NODE_ID"]
        self._wait_between_heartbeats = config["WAIT_BETWEEN_HEARTBEAT"]
        self._election_port = config["ELECTION_PORT"]
        self._leader_comunicaton_port = config["LEADER_COMUNICATION_PORT"]
        self._leader_discovery_port = config["LEADER_DISCOVERY_PORT"]
        self._middleware = middleware
        self._monitor_access_port = config["PORT"]
        self._amount_of_monitors = config["AMOUNT_OF_MONITORS"]

        self._got_sigterm = threading.Event()

        self._leader_election = LeaderElection(self._node_id, self._election_port, self._amount_of_monitors)
        self._leader_discovery = LeaderDiscoveryService(self._leader_discovery_port, self._leader_election)

        self._discovery_thread = threading.Thread(target=self._leader_discovery.run)
        self._discovery_thread.start()

        signal.signal(signal.SIGTERM, self._sigterm_handler)
        signal.signal(signal.SIGINT, self._sigterm_handler)

        self._watchdog_setup()


    def _watchdog_setup(self):
        
        with open("watchdog_status/node_names.json", "r") as file:
            nodes_names = json.load(file)
            self._nodes = {key: None for key in nodes_names}
        
        with open("watchdog_status/monitor_names.json", "r") as file:
            peer_names = json.load(file)
            self._peers = {key: None for key in peer_names if key != f"{WATCHDOG_NAME_PREFIX}{self._node_id}"}
           

    def start(self):
        logging.info("Looking for active leader")

        self._leader_election.look_for_leader()
  
        while not self._got_sigterm.is_set():
            if self._leader_election.get_leader_id() is None:
                logging.info("There was no active leader, Starting election")
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
        peer_listener_thread = threading.Thread(target=self._listen_for_peers, daemon=True)
        peer_listener_thread.start()

        try:
            self._middleware.bind(("", self._monitor_access_port))
        except OSError as e:
            if not self._got_sigterm.is_set():
                logging.error(f"[LEADER] GOT ERROR WHILE BINDING PORT: {e}")
            return

        handlers = {} #save the handlers here to allow reconnections

        self._middleware.set_timeout(1)
        _last_node_check_time = time.time()
        while not self._got_sigterm.is_set():
            try:
            
                conn, _ = self._middleware.accept_connection()
                node_name = conn.recv()
                
                if not node_name in handlers:
                    logging.info(f"[LEADER] Node {node_name} connected.")
                    
                    handler = NodeHandler(conn, node_name, self._got_sigterm, self._wait_between_heartbeats)
                    handlers[node_name] = handler

                    conn.send(REGISTRATION_CONFIRM)

                    node_thread = threading.Thread(target=handler.start, daemon=True)
                    node_thread.start()
                    self._nodes[node_name] = node_thread
                
                else:
                    logging.info(f"[LEADER] Node {node_name} re-connected.")
                    conn.send(REGISTRATION_CONFIRM)
                    handlers[node_name].set_new_connection(conn)
                    
                _last_node_check_time = self._check_and_reconnect_nodes(_last_node_check_time)
                    
            except TCPMiddlewareTimeoutError:
                _last_node_check_time = self._check_and_reconnect_nodes(_last_node_check_time)

            except (OSError, ConnectionError) as e:
                if not self._got_sigterm.is_set():
                    logging.error(f"ERROR: {e}")
                    break
        
        for handler in handlers.values():
            handler.close()
            
        self._release_nodes_threads()
        peer_listener_thread.join()
        

    def _listen_to_leader(self):

        leader_id = self._leader_election.get_leader_id()
        leader_addr = (f"{WATCHDOG_NAME_PREFIX}{leader_id}", self._leader_comunicaton_port)

        middleware = UDPMiddleware(send_retries=NUMBER_OF_RETRIES)
        
        while not self._got_sigterm.is_set(): 
            if self._got_sigterm.is_set():
                break

            message = f"{HEARTBEAT_MESSAGE},{WATCHDOG_NAME_PREFIX}{self._node_id}"
            if not middleware.send_message(message, leader_addr):
                logging.info("The monitor is down")
                self._leader_election.set_leader_death()
                break
            
            logging.debug("Sending heartbeat")
            self._got_sigterm.wait(TIME_BETWEEN_HEARTBEATS)

        middleware.close()


    def _listen_for_peers(self):

        middleware = UDPMiddleware()
        middleware.bind(("", self._leader_comunicaton_port))
        middleware.set_receiver_timeout(timeout=2)
        
        #set start_time to check if a node never connected
        for peer_name in self._peers.keys():
            self._peers[peer_name] = time.time()

        while not self._got_sigterm.is_set():
            try:
                msg: str = middleware.receive_message(length=10)
                _, node_name = msg.split(",")
                logging.debug(f"NODE {node_name} SENT HEARTBEAT")
                self._peers[node_name] = time.time()
                #i need to check here if TIME_BETWEEN_HEARTBEATS < middleware timeout
                self._check_peer_status() 

            except UDPMiddlewareTimeoutError:
                self._check_peer_status() 
            except (OSError, ConnectionError) as e:
                if not self._got_sigterm.is_set():
                    logging.error(f"Connection closed {e}") 


    def _check_peer_status(self):
        current_time = time.time()
        for peer_name, last_heartbeat in self._peers.items():
            if  current_time - last_heartbeat > MAX_MONITOR_TIMEOUT: 
                self._restart_fallen_node(peer_name)
                self._peers[peer_name] = time.time()       


    def _check_and_reconnect_nodes(self, _last_node_check_time):
        current_time = time.time()
        if current_time - _last_node_check_time > TIMEOUT_BEFORE_FALLEN_CHECK: 
            self._reconnect_fallen_nodes()
            return current_time
        
        return _last_node_check_time


    def _restart_fallen_node(self, node_name):
        logging.info(f"Node {node_name} never reconnected. Starting it...")
        subprocess.run(['docker', 'stop', node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(['docker', 'start', node_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def _reconnect_fallen_nodes(self):
        logging.debug("Checking for nodes that never connected")
        for node, conn in self._nodes.items():
            if not conn:
                self._restart_fallen_node(node)
                

    def _release_nodes_threads(self):
        for thread in self._nodes.values():
            if thread is not None:
                thread.join()
      

    def _sigterm_handler(self, signal, frame):
        self._got_sigterm.set()
        self._middleware.close()
        self._leader_discovery.close()
        self._discovery_thread.join()
        self._leader_election.stop()
        

