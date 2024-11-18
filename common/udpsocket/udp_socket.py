import socket

AMOUNT_OF_RETRIES = 3
ACK_MESSAGE = "ACK"

class UDPSocket:
    """
    UDP SOCKET Implementation using stop & and wait.

    If you want to send messages, you cant receive them
    on the same socket because messages can get mixed while waiting for the sent message ACK
    """
    def __init__(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._timeout = 1

    def bind(self, address):
        self._socket.bind(address)

    def send_message(self, message, address):

        for i in range(1, AMOUNT_OF_RETRIES+1):
            try:
                self._sendall(message, address)
                self._socket.settimeout(self._timeout)
                msg, _ = self._safe_recv(3)

                if msg != ACK_MESSAGE: raise ConnectionError
                
                break

            except socket.timeout as _:
                if i == AMOUNT_OF_RETRIES: 
                    return False #receptor is disconnected

            except socket.gaierror as _:
                #the client is disconnected
                return False


            finally:
                self._socket.settimeout(None)

        return True


    def recv_message(self, amount_of_bytes):

        msg, addr = self._safe_recv(amount_of_bytes)
        self._sendall(ACK_MESSAGE, addr)

        return msg


    def _sendall(self, message:str, address):

        encoded_msg = message.encode("utf-8")
        bytes_sent = 0
        bytes_to_send = len(encoded_msg)

        while bytes_sent < bytes_to_send:
            size_sent = self._socket.sendto(encoded_msg[bytes_sent:], address)
            bytes_sent += size_sent


    def _safe_recv(self, amount_of_bytes):
    
        message = b""
        addr = None

        while len(message) < amount_of_bytes:
            chunk, addr = self._socket.recvfrom(1024)
            if not chunk:
                raise ConnectionError("Connection closed or no data received")
            message += chunk

        return message.decode("utf-8"), addr
    
    def close(self):
        self._socket.close()




