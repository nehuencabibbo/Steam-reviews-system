from common.server_socket.server_socket import ServerSocket
from common.leader_election.leader_election import LeaderElection

import socket
import logging

LEADER_ELECTION_RUNNING = "F"
BACKLOG = 30

class LeaderDiscoveryService:

    def __init__(self, port, leader_election: LeaderElection):
        self._socket = ServerSocket(port)
        self._leader_election = leader_election

        self._stop = False

    def run(self):

        self._socket.bind(BACKLOG)
        self._socket.settimeout(5)

        while not self._stop:
            
            try:
                conn = self._socket.accept_connection()
   
                leader_id = self._leader_election.get_leader_id()
                if self._leader_election.is_running() or leader_id == None:
                    conn.send(LEADER_ELECTION_RUNNING)
                else:
                    conn.send(f"{leader_id}")

                conn.close()
            except socket.timeout as _:
                continue
            except (OSError, ConnectionError) as _:
                if not self._stop:
                    logging.error("Unexpected socket close")
                break
        
        self._socket.close()

    def close(self):
        self._stop = True
        self._socket.close()
            

            