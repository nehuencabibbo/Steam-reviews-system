import threading
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from udpsocket.udp_socket import UDPSocket

ELECTION_COMMAND = "E"
OK_COMMAND = "O"
LEADER_COMMAND = "C"

LIDER_ELECTION_TIMEOUT = 5


#TODO: How one node detects the failure of the leader? -> when sync the other nodes
class LeaderElection:

    def __init__(self, node_id, election_port):
        self._id = node_id
        self._amount_of_nodes = 3 #dont hardcode this
        
        self._leader = None
        self._got_ok = threading.Event()
        self._not_running_election = threading.Event()
        self._not_running_election.set()

        self._stop = False

        self._sender_lock = threading.Lock() 
        self._sender_socket = UDPSocket() #it is not necessary to bind it
        self._receiver_socket = UDPSocket()
        self._receiver_socket.bind(("", election_port + node_id))

        self._election_port = election_port

        threading.Thread(target=self.receive, daemon=True).start()
    
    
    def start_leader_election(self):
        # si empezó la eleccion, no la hago otra vez
        
        if not self._not_running_election.is_set():
            logging.info(f"NODE {self._id} | election was running")
            return
        
        logging.info(f"NODE {self._id} | running election")
        self._not_running_election.clear()

        self._send_election_message()

        # wait until timeout to check if you received an OK or not
        self._got_ok.wait(LIDER_ELECTION_TIMEOUT)
        if self._got_ok.is_set():
            logging.info(f"NODE {self._id} | Got OK, waiting for leader")
            return    
        
        logging.info(f"NODE {self._id} | I won the election")
        # I am the leader
        self._leader = self._id
        self._send_leader_message()
        self._not_running_election.set()


    def receive(self):
        #esto se recibe todo en otro thread aparte. #Al mismo tiempo que envio, debo poder recibir los mensajes
        
        while not self._stop:
            #msg : bytes = self._socket.recv(1024) #.receive()
            try:
                msg = self._receiver_socket.recv_message(3)
            except OSError as e:
                if not self._stop:
                    logging.error(f"Got error while listening peers {e}")
                return

            #command, node_id = msg.decode("utf-8").split(",")
            command, node_id = msg.split(",")
            node_id = int(node_id)

            logging.info(f"NODE {self._id} | Got message: {command} from {node_id}")
            if command == ELECTION_COMMAND:
                if self._id > node_id:
                    self._send_ok_message(node_id)

            elif command == OK_COMMAND:
                self._got_ok.set()

            elif command == LEADER_COMMAND:
                self._leader = node_id
                self._got_ok.clear()
                self._not_running_election.set()


    def i_am_leader(self):
        self._not_running_election.wait()
        return self._leader == self._id


    def get_leader_id(self):
        return self._leader
    

    def stop(self):
        self._stop = True
        self._receiver_socket.close()
        self._sender_socket.close()
        #nodes waiting for election can have a greful exit during the election
        self._not_running_election.set() 


    def _send_election_message(self):
        message = f'E,{self._id}'
        for node_id in range(self._id + 1, self._amount_of_nodes):
            if self._stop:
                return
            with self._sender_lock:
                self._sender_socket.send_message(message, self._node_id_to_addr(node_id))


    def _send_leader_message(self):
        message = f'C,{self._id}'
        for node_id in range(0, self._amount_of_nodes):
            if self._stop:
                return
            if node_id == self._id:
                continue

            with self._sender_lock:
                self._sender_socket.send_message(message, self._node_id_to_addr(node_id))


    def _send_ok_message(self, node_id):   
        if self._stop:
            return
        
        message = f'O,{self._id}'
        with self._sender_lock:
            self._sender_socket.send_message(message, self._node_id_to_addr(node_id))


    def _node_id_to_addr(self, node_id):
        return (f"watchdog_{node_id}", self._election_port + node_id)

    


