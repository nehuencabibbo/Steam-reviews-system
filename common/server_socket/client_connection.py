from common.protocol.protocol import Protocol
LENGTH_BYTES =  4

class ClientConnection:

    #TODO: try catch de errores
    def __init__(self, socket, timeout = 2):
        self._socket = socket
        self._stop = False
        self._socket.settimeout(timeout)

    def send(self, message: str):
        encoded_msg = Protocol.encode([message])

        bytes_to_send = len(encoded_msg)

        while bytes_to_send > 0:

            bytes_sent = self._socket.send(encoded_msg)
            bytes_to_send -= bytes_sent

        return True
    
    def recv(self):
        
        bytes_received = 0
        message = b""
        message_len = self._recv_message_length()
        while bytes_received < message_len:

            message += self._socket.recv(message_len - bytes_received)
            bytes_received += len(message)

        decoded_message = message.decode("utf-8")

        return decoded_message
    
    def _recv_message_length(self):

        bytes_left_to_receive = LENGTH_BYTES
        message = b""
        while bytes_left_to_receive < 0:

            message += self._socket.recv(bytes_left_to_receive)

            bytes_left_to_receive -= len(message)

        return int.from_bytes(message[:4], "big", signed=False)
    
    def close(self):
        self._stop = True
        self._socket.close()
