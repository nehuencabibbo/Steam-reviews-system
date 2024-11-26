from common.server_socket.tcp_middleware import TCPMiddleware, TCPMiddlewareTimeoutError
from common.leader_election.leader_election import LeaderElection

import logging

LEADER_ELECTION_RUNNING = "F"
BACKLOG = 30

class LeaderDiscoveryService:

    def __init__(self, port, leader_election: LeaderElection):
        self._middleware = TCPMiddleware()
        self._port = port
        self._leader_election = leader_election
        self._stop = False

    def run(self):

        self._middleware.bind(("", self._port), BACKLOG)
        self._middleware.set_timeout(1) # to check if i need to close
        conn = None
        while not self._stop:
            
            try:
                conn, _ = self._middleware.accept_connection()
   
                leader_id = self._leader_election.get_leader_id()
                if self._leader_election.is_running() or leader_id == None:
                    conn.send(LEADER_ELECTION_RUNNING)
                else:
                    conn.send(f"{leader_id}")

            except TCPMiddlewareTimeoutError:
                continue
            except (OSError, ConnectionError) as e:
                if not self._stop:
                    logging.error(f"Unexpected socket close: {e}")
                break
            finally:
                if conn:
                    conn.close()
        
        self._middleware.close()

    def close(self):
        self._stop = True
        self._middleware.close()
            

            