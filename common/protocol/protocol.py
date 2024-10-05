import unittest

from typing import *

FIELD_LENGTH_BYTES_AMOUNT = 4

class Protocol: 
    @staticmethod
    def encode(row: List[str]) -> bytes:
        result = b''
        for field in row: 
            encoded_field = field.encode('utf-8')
            encoded_field_length = len(encoded_field).to_bytes(FIELD_LENGTH_BYTES_AMOUNT, 'big', signed=False)
            result += encoded_field_length
            result += encoded_field

        return result

    @staticmethod
    def decode(message: bytes) -> List[str]:
        result = []
        while len(message) > 0:
            field_length = int.from_bytes(message[:4], 'big', signed=False)
            message = message[4:]

            field = message[:field_length]
            field = field.decode('utf-8')
            result.append(field)
            message = message[field_length:]

        return result 
    
class TestProtocol(unittest.TestCase):
    def test_encode(self):
        # Test data
        row = ["Hello", "World"]
        expected_bytes = b'\x00\x00\x00\x05Hello\x00\x00\x00\x05World'
        
        # Call the encode method
        encoded = Protocol.encode(row)
        
        # Assert the result
        self.assertEqual(encoded, expected_bytes)
    
    def test_decode(self):
        # Test data
        message = b'\x00\x00\x00\x05Hello\x00\x00\x00\x05World'
        expected_list = ["Hello", "World"]
        
        # Call the decode method
        decoded = Protocol.decode(message)
        
        # Assert the result
        self.assertEqual(decoded, expected_list)

    def test_correct_encode_decode(self):
        message = ["Hello", "World"]

        encoded_message = Protocol.encode(message)
        decoded_message = Protocol.decode(encoded_message)

        self.assertEqual(message, decoded_message)

if __name__ == '__main__':
    unittest.main()