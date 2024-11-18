import threading
import socket
import logging

ELECTION_COMMAND = "E"
OK_COMMAND = "O"
LEADER_COMMAND = "C"

LIDER_ELECTON_TIMEOUT = 10


#TODO: How one node detects the failure of the leader? -> when sync the other nodes
class LeaderElection:

    def __init__(self, node_id, election_port):
        self._id = node_id
        self._amount_of_nodes = 3 #dont hardcode this
        self._leader = None #it should be shared between threads. SHould not be a problem due to event locking
        self._got_ok = threading.Event()
        self._not_running_election = threading.Event()
        self._not_running_election.set()

        self._stop = False

        #TODO: Hacer stop and wait for reliability or something else /selective repeat?
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.bind(('', election_port + node_id)) #so i can receive msg here

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
        self._got_ok.wait(LIDER_ELECTON_TIMEOUT)
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
            msg : bytes = self._socket.recv(1024) #.receive()

            command, node_id = msg.decode("utf-8").split(",")
            node_id = int(node_id)

            logging.info(f"NODE {self._id} | Got message: {command} from {node_id}")
            if command == ELECTION_COMMAND:
                if self._id > node_id:
                    self._send_ok_message(node_id)
                    #con multithreading no hace falta, pero debería ver donde hacerle join
                    threading.Thread(target=self.start_leader_election(), daemon=True).start() 

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
        self._socket.close()


    def _send_election_message(self):
        message = f'E,{self._id}'.encode("utf-8")
        for node_id in range(self._id + 1, self._amount_of_nodes):
            if self._stop:
                return
            self._socket.sendto(message, self._node_id_to_addr(node_id))


    def _send_leader_message(self):
        message = f'C,{self._id}'.encode("utf-8")
        for node_id in range(0, self._amount_of_nodes):
            if self._stop:
                return
            if node_id == self._id:
                continue
            self._socket.sendto(message, self._node_id_to_addr(node_id))


    def _send_ok_message(self, node_id):   
        if self._stop:
            return
        
        message = f'O,{self._id}'.encode("utf-8")
        self._socket.sendto(message, self._node_id_to_addr(node_id))


    def _node_id_to_addr(self, node_id):
        return (f"watchdog_{node_id}", self._election_port + node_id)

    


