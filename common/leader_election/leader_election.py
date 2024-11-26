import threading
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.udpsocket.udp_middleware import UDPMiddleware, UDPMiddlewareTimeoutError

ELECTION_COMMAND = "E"
OK_COMMAND = "O"
LEADER_COMMAND = "C"
LEADER_QUERY_COMMAND = "L"
LEADER_QUERY_RESPONSE = "R"

LEADER_ELECTION_TIMEOUT = 7
LEADER_QUERY_TIMEOUT = 3
LEADER_ELECTED_TIMEOUT = 15
MESSAGE_LENGTH = 3

CHECK_STOP_TIMEOUT = 1

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
        self._election_port = election_port
        self._stop = threading.Event()

        self._middleware = UDPMiddleware()
        self._middleware.bind(("", election_port + node_id))
        self._setup()
        self._receiver_thread = threading.Thread(target=self.receive, daemon=True)
        self._receiver_thread.start()


    def _setup(self):
        for i in range(self._amount_of_nodes):
            if i == self._id:
                continue
            addr = self._node_id_to_addr(i)
            self._middleware.add_addr_to_broadcast(addr)
    

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
        self._send_leader_message() # takes too much time to send to every node. Do it on other thread?
        self._not_running_election.set()


    def receive(self):
        self._middleware.set_receiver_timeout(CHECK_STOP_TIMEOUT)

        while not self._stop.is_set():
            try:

                msg: str = self._middleware.receive_message(MESSAGE_LENGTH) 
            except UDPMiddlewareTimeoutError:
                if self._stop.is_set() or not self._got_ok.is_set():
                    continue

                logging.info("No response from leader candidate.")
                self._handle_no_candidate_response()

            except OSError as e:
                if not self._stop.is_set():
                    logging.error(f"Got error while listening peers: {e}")
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
        self._middleware.broadcast(message)
        self._leader_found.wait(LEADER_QUERY_TIMEOUT)


    def stop(self):
        self._stop.set()
        self._middleware.close()
        self._not_running_election.set()
        self._receiver_thread.join()


    def _handle_no_candidate_response(self):
        self._got_ok.clear()
        self._not_running_election.set()
        self._middleware.set_receiver_timeout(None)


    def _handle_message(self, message, node_id):

        if message == ELECTION_COMMAND:
            if self._id > node_id:
                self._send_ok_message(node_id)

        elif message == OK_COMMAND:
            self._got_ok.set()
            # so i can detect if the one that will be the leader crashed or not
            # if that happens it nevers send the coordinator message
            # and i cant know if it disconnected or not
            self._middleware.set_receiver_timeout(LEADER_ELECTED_TIMEOUT)

        elif message == LEADER_COMMAND:
            with self._leader_lock:
                self._leader = node_id
            self._got_ok.clear()
            self._not_running_election.set()
            self._middleware.set_receiver_timeout(CHECK_STOP_TIMEOUT) #None
        
        elif message == LEADER_QUERY_COMMAND:
            with self._leader_lock:
                if not self._leader is None:
                    self._send_leader_id_message(node_id)
            
        elif message == LEADER_QUERY_RESPONSE:
            with self._leader_lock:
                self._leader = node_id
            self._leader_found.set()


    def _send_election_message(self):
        message = f'{ELECTION_COMMAND},{self._id}'
        for node_id in range (self._id + 1, self._amount_of_nodes):
            node_addr = self._node_id_to_addr(node_id)
            self._middleware.send_message(message, node_addr)


    def _send_leader_message(self):
        message = f'{LEADER_COMMAND},{self._id}'
        self._middleware.broadcast(message)


    def _send_ok_message(self, node_id):   
        if self._stop.is_set():
            return
        
        message = f'{OK_COMMAND},{self._id}'
        node_addr = self._node_id_to_addr(node_id)
        self._middleware.send_message(message, node_addr)


    def _send_leader_id_message(self, node_id):
        if self._stop.is_set():
            return
        
        message = f'{LEADER_QUERY_RESPONSE},{self._leader}'
        node_addr = self._node_id_to_addr(node_id)
        self._middleware.send_message(message, node_addr)


    def _node_id_to_addr(self, node_id):
        return (f"watchdog_{node_id}", self._election_port + node_id)
