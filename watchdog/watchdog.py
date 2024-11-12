"""
TODO: buscar otra forma de saber cuales son todos los nodos que puedo levantar otra vez

El healthchecker va a funcionar como un servidor: 
- Los distintos nodos se conectan a él y le mandan su nombre # [largo_nombre nombre del nodo] -> def protocolo?
- OPCIONAL: si no uso zmq, usar un thread por conexion del nodo
- Cada vez que el healthchecker envia B (u otro mensaje), el cliente responde con "A". (es lo mismo el mensaje, lo importante es que responda)

Si el cliente no responde luego de un tiempo o si se detecta que se cayó la conexión (error en socket al recibir),
el healthchecker vuelve a levantar el contenedor con subprocess.run(['docker', 'start', <node_name>]).
Esto implica que el nodo va a volver a enviar su id , lo cual permite actualizar la conexión que le corresponde el nodo.
El thread que detecta la desconexion sale y cierra el socket, ya que se va a lanzar un nuevo thread-> donde hago el join del hilo -> al aceptar un nuevo nodo :D
Al aceptar un nuevo nodo, si ya existía, hago el join del hilo y lo reemplazo con el nuevo hilo

Que pasa si se cae el lider cuando detecta la desconexión? En ese caso, el prox lider debe darse cuenta de que se cayó un nodo -> 
Puedo tener un diccionario con los nodos que me paso el lider y otro con los que yo recibo? Entonces si falta alguno, lo levanto antes de iniciar

A todo esto, falta agregar una elección de lider por si se cae el healthchecker.
En ese caso, los clientes deben intentar reconectarse al healthchcker (n reintentos)


Que pasa si se cae un nodo durante la elección de lider? -> si los hijos recibieron todos los nombres, no pasa nada
Que pasa si se cae lider antes de enviar que un nodo se conectó a los "hijos"? -> nada porque los demas deben reconectarse
Pero si se cae el lider antes de que se envien todos los nombres de los nodos y durante la elección se cae uno de los nodos
cuyo nombre no se envio -> entonces hay problema
"""
from common.server_socket.server_socket import ServerSocket
from node_handler import NodeHandler

#import threading
import multiprocessing
import signal
import logging

class Watchdog:
    def __init__(self, server_socket: ServerSocket, config: dict):
        self._server_socket = server_socket
        self._threads = {}
        self._got_sigterm = multiprocessing.Event()
        self._wait_between_heartbeats = config["WAIT_BETWEEN_HEARTBEAT"]
        signal.signal(signal.SIGTERM, self._sigterm_handler)

    def start(self):
        #TODO: pasar por env todos los nombres y cantidad, asi se que si falta uno luego de n segs (en accept), lo busco y lo levanto
        try:
            while not self._got_sigterm.is_set():

                conn = self._server_socket.accept_connection(5) #set timeout for connection
            
                node_name = conn.recv()

                logging.info(f"Node {node_name} connected.")
                
                handler = NodeHandler(conn, node_name, self._got_sigterm, self._wait_between_heartbeats)
                #thread = threading.Thread(target=handler.start, args=())
                thread = multiprocessing.Process(target=handler.start, args=())
                thread.start()
                
                if node_name in self._threads:
                    self._threads[node_name].join()

                self._threads[node_name] = thread
                #self._handlers[node_name] = handler
        except (OSError, ConnectionError) as e:
            if not self._got_sigterm.is_set():
                logging.error(f"ERROR: {e}")
                raise
        finally:
            [thread.join() for thread in self._threads.values()] #error here
            #[handler.close() for handler in self._hanlders]
                
    
    def _sigterm_handler(self, signal, frame):
        self._got_sigterm.set()
        self._server_socket.close()
        
