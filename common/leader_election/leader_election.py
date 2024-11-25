import threading
import logging
import sys
import os
import socket
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from udpsocket.udp_socket import UDPSocket

ELECTION_COMMAND = "E"
OK_COMMAND = "O"
LEADER_COMMAND = "C"
LEADER_QUERY_COMMAND = "L"
LEADER_QUERY_RESPONSE = "R"

LEADER_ELECTION_TIMEOUT = 7
LEADER_QUERY_TIMEOUT = 3
LEADER_ELECTED_TIMEOUT = 15
MESSAGE_LENGTH = 3

class LeaderElection:

    def __init__(self, node_id, election_port, amount_of_nodes):
        self._id = node_id
        self._amount_of_nodes = amount_of_nodes
        
        self._leader_lock = threading.Lock()
        self._leader = None
        
        self._got_ok = threading.Event()
        self._leader_found = threading.Event()
        self._not_running_election = threading.Event()
        self._not_running_election.set()
        
        self._stop = False

        self._sender_lock = threading.Lock() 
        self._sender_socket = UDPSocket(timeout=0.5, amount_of_retries=2)
        self._receiver_socket = UDPSocket()
        self._receiver_socket.bind(("", election_port + node_id))

        self._election_port = election_port

        threading.Thread(target=self.receive, daemon=True).start()
    
    
    def start_leader_election(self):

        if not self._not_running_election.is_set():
            logging.info(f"NODE {self._id} | election was running")
            return
        
        logging.info(f"NODE {self._id} | running election")
        self._not_running_election.clear()

        self._send_election_message()

        self._got_ok.wait(LEADER_ELECTION_TIMEOUT)
        if self._got_ok.is_set():
            logging.info(f"NODE {self._id} | Got OK, waiting for leader")
            return    
        
        logging.info(f"NODE {self._id} | I won the election")
        with self._leader_lock:
            self._leader = self._id
        self._send_leader_message()
        self._not_running_election.set()


    def receive(self):

        while not self._stop:
            try:
                msg = self._receiver_socket.recv_message(MESSAGE_LENGTH)

                if self._stop:
                    return
                
            except socket.timeout:
                logging.info("No response from leader candidate.")
                #leader will be none, and it will be handled form the outside
                self._handle_no_candidate_response()

            except OSError as e:
                if not self._stop:
                    logging.error(f"Got error while listening peers {e}")
                return

            command, node_id = msg.split(",")
            node_id = int(node_id)
            logging.debug(f"NODE {self._id} | Got message: {command} from {node_id}")
            self._handle_message(command, node_id)


    def i_am_leader(self):
        self._not_running_election.wait()
        return self._leader == self._id


    def get_leader_id(self):
        with self._leader_lock:
            return self._leader
    
    
    def is_running(self):
        return not self._not_running_election.is_set()


    def set_leader_death(self):
        with self._leader_lock:
            self._leader = None
    

    def look_for_leader(self):
        message = f'{LEADER_QUERY_COMMAND},{self._id}'
        self._broadcast(message)
        self._leader_found.wait(LEADER_QUERY_TIMEOUT)


    def stop(self):
        self._stop = True
        self._receiver_socket.close()
        self._sender_socket.close()
        self._not_running_election.set() 


    def _handle_no_candidate_response(self):
        self._got_ok.clear()
        self._not_running_election.set()
        self._receiver_socket.settimeout(None)


    def _handle_message(self, message, node_id):

        if message == ELECTION_COMMAND:
            if self._id > node_id:
                self._send_ok_message(node_id)

        elif message == OK_COMMAND:
            self._got_ok.set()
            # so i can detect if the one that will be the leader crashed or not
            # if that happens it nevers send the coordinator message
            # and i cant know if it disconnected or not
            self._receiver_socket.settimeout(LEADER_ELECTED_TIMEOUT)

        elif message == LEADER_COMMAND:
            with self._leader_lock:
                self._leader = node_id
            self._got_ok.clear()
            self._not_running_election.set()
            self._receiver_socket.settimeout(None) 
        
        elif message == LEADER_QUERY_COMMAND:
            with self._leader_lock:
                if not self._leader is None:
                    self._send_leader_id_message(node_id)
            
        elif message == LEADER_QUERY_RESPONSE:
            with self._leader_lock:
                self._leader = node_id
            self._leader_found.set()


    def _send_message(self, message, addr):
        with self._sender_lock:
            try:
                self._sender_socket.send_message(message, addr)
            except (OSError, ConnectionError):
                return
            

    def _broadcast(self, message):
         for node_id in range(0, self._amount_of_nodes):
            if self._stop:
                return
            if node_id == self._id:
                continue
            
            node_addr = self._node_id_to_addr(node_id)
            self._send_message(message, node_addr)



    def _send_election_message(self):
        message = f'{ELECTION_COMMAND},{self._id}'
        for node_id in range(self._id + 1, self._amount_of_nodes):
            if self._stop:
                return
            node_addr = self._node_id_to_addr(node_id)
            self._send_message(message, node_addr)


    def _send_leader_message(self):
        message = f'{LEADER_COMMAND},{self._id}'
        self._broadcast(message)



    def _send_ok_message(self, node_id):   
        if self._stop:
            return
        
        message = f'{OK_COMMAND},{self._id}'
        node_addr = self._node_id_to_addr(node_id)
        self._send_message(message, node_addr)


    def _send_leader_id_message(self, node_id):
        if self._stop:
            return
        
        message = f'{LEADER_QUERY_RESPONSE},{self._leader}'
        node_addr = self._node_id_to_addr(node_id)
        self._send_message(message, node_addr)


    def _node_id_to_addr(self, node_id):
        return (f"watchdog_{node_id}", self._election_port + node_id)

    


