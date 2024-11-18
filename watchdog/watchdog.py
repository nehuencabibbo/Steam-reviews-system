from common.server_socket.server_socket import ServerSocket
from common.server_socket.client_connection import ClientConnection
from node_handler import NodeHandler
from common.leader_election.leader_election import LeaderElection


import multiprocessing
import signal
import logging
import threading
import socket
from time import sleep

WAIT_FOR_CONNECTIONS = 5

class Watchdog:
    def __init__(self, server_socket: ServerSocket, config: dict):
        #server socket must e initialize only if its a leader
        #so i should not receive it as a parameter (or at least i should not connect on constructor)
        self._node_id = config["NODE_ID"]
        self._wait_between_heartbeats = config["WAIT_BETWEEN_HEARTBEAT"]
        self._election_port = config["ELECTION_PORT"]
        self._leader_comunicaton_port = config["LEADER_COMUNICATION_PORT"]

        self._server_socket = server_socket
        self._peers_socket = ServerSocket(self._leader_comunicaton_port)

        self._nodes = {}
        self._got_sigterm = multiprocessing.Event()

        self._peers_lock = threading.Lock()
        self._peers: list[ClientConnection] = [] #so i can send updates to them

        self._leader_election = LeaderElection(self._node_id, self._election_port)
        signal.signal(signal.SIGTERM, self._sigterm_handler)


    def start(self):
        # wait for everyone to init (with stop and wait this is not an issue)
        sleep(1)
        # TODO: allow reconections from fallen nodes without triggering election -> Ask if there is a leader before entering?
    
        while not self._got_sigterm.is_set():

            self._leader_election.start_leader_election()
            
            if self._leader_election.i_am_leader():
                logging.info("I'm the leader")

                thread = threading.Thread(target=self._listen_for_peers, daemon=True)
                thread.start()

                self._server_socket.bind()
                self._monitor_nodes()

                thread.join()
            else:
                logging.info("I'm not the leader")
                sleep(1) # wait for leader to settle
                self._listen_to_leader()


    def _monitor_nodes(self):

        #TODO: If a node registered but the monitor fails, the new leader must check the node connected. 

        # If the node never registered, it will be imposible to restart it because it does not exist for the monitor
        try:
            while not self._got_sigterm.is_set():

                #TODO: MAKE TIMEOUT FOR ACCEPT SO IT CAN CHECK NODES THAT DIDNT RECONNECT
                conn = self._server_socket.accept_connection(5)
            
                node_name = conn.recv()

                # TODO: send nodes names to peers
                self._send_registration_to_peers(node_name)
                # send registration confirmation message
                # conn.send_ack()
                logging.info(f"Node {node_name} connected.")

                handler = NodeHandler(conn, node_name, self._got_sigterm, self._wait_between_heartbeats)
                thread = multiprocessing.Process(target=handler.start, daemon=True)
                thread.start()
                
                if node_name in self._nodes:
                    self._nodes[node_name].join()

                self._nodes[node_name] = thread

        except (OSError, ConnectionError) as e:
            if not self._got_sigterm.is_set():
                logging.error(f"ERROR: {e}")
        finally:
            [thread.join() for thread in self._nodes.values()] #error here
    

    def _listen_to_leader(self):
        #TODO: connect to monitor leader so it can monitor this nodes.
        leader_id = self._leader_election.get_leader_id()

        # TODO: check error when leader fails
        leader_connection = self._connect_to_leader(leader_id) 
        logging.info("Connected to leader")
        while not self._got_sigterm.is_set():
            try:
                node_name = leader_connection.recv()
                logging.info(f"Leader sent a message: {node_name}")
                self._nodes[node_name] = None # i register it, but it has no thread since child does not monitor
            except (OSError, ConnectionError):
                if not self._got_sigterm.is_set():
                    logging.info("The leader is down")
                return
    

    def _listen_for_peers(self):
        #set this socket on the constructor so i can close it 
        self._peers_socket.bind()
        self._peers_socket.settimeout(5)

        while not self._got_sigterm.is_set():
            try:
                child = self._peers_socket.accept_connection()
                logging.info("A child conected :D")
                with self._peers_lock:
                    self._peers.append(child)
                    #TODO: syncronize with the leader data inside the lock?
            except socket.timeout:
                continue
            except (OSError, ConnectionError) as _:
                return


    def _connect_to_leader(self, leader_id):
        leader_ip = f"watchdog_{leader_id}"
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((leader_ip, self._leader_comunicaton_port))
        return ClientConnection(client_socket, None)
    

    def _send_registration_to_peers(self, node_name):
        logging.info("Sending registration to peers")
        #TODO: send in order from higher id to lower id
        with self._peers_lock:
            for i in range(0, len(self._peers)):
                try:
                    self._peers[i].send(node_name) 
                except (OSError, ConnectionError) as _:
                    if self._got_sigterm: return
                    self._peers.pop(i)


    def _sigterm_handler(self, signal, frame):
        self._got_sigterm.set()
        self._server_socket.close()
        self._leader_election.stop()
        self._peers_socket.close()

        with self._peers_lock:
            for peer in self._peers:
                peer.close()
        
