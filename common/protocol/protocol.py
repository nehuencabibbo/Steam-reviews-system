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
    
    @staticmethod
    def encode_batch(batch: List[List[str]]) -> bytes:
        result = b''
        for row in batch:
            encoded_row = Protocol.encode(row)
            encoded_row_length = len(encoded_row).to_bytes(FIELD_LENGTH_BYTES_AMOUNT, 'big', signed=False)
            result += encoded_row_length
            result += encoded_row

        return result
    
    @staticmethod
    def decode_batch(message: bytes) -> List[List[str]]:
        result = []
        while len(message) > 0:
            row_length = int.from_bytes(message[:4], 'big', signed=False)
            message = message[4:]

            row = message[:row_length]
            decoded_row = Protocol.decode(row)
            result.append(decoded_row)
            message = message[row_length:]

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

    def test_batch_encode(self):
        batch = [["Hello"], ["World"]]
        expected_batch = b'\x00\x00\x00\x09\x00\x00\x00\x05Hello\x00\x00\x00\x09\x00\x00\x00\x05World'

        encoded = Protocol.encode_batch(batch)

        self.assertEqual(encoded, expected_batch)

    def test_batch_decode(self):
        batch = b'\x00\x00\x00\x09\x00\x00\x00\x05Hello\x00\x00\x00\x09\x00\x00\x00\x05World'
        expected_list = [["Hello"], ["World"]]

        decoded = Protocol.decode_batch(batch)

        self.assertEqual(decoded, expected_list)

    def test_batch_encoode_decode(self):
        batch = [["Hello", "PEPE"], ["World", "Cup"]]

        encoded = Protocol.encode_batch(batch)
        decoded = Protocol.decode_batch(encoded)

        self.assertEqual(decoded, batch)

if __name__ == '__main__':
    unittest.main()