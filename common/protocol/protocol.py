import logging
import unittest

from typing import *

FIELD_LENGTH_BYTES_AMOUNT = 4

class ProtocolError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message


class Protocol:
    @staticmethod
    def __remove_and_validate_checksum(message: bytes) -> bytes: 
        checksum = message[:4]
        message = message[4:]

        checksum = int.from_bytes(checksum, byteorder='big', signed=False)

        if checksum != len(message):
            raise ProtocolError((
                f'ERROR: Checksum does not match, '
                f'expected: {checksum}, got: {len(message)}'
            ))
        
        return message

    @staticmethod
    def _add_checksum(message: bytes) -> bytes:
        length = len(message).to_bytes(4, byteorder='big', signed=False)

        return length + message
    
    @staticmethod
    def encode(row: List[str], add_checksum=False) -> bytes:
        result = b""
        for field in row:
            encoded_field = field.encode("utf-8")
            encoded_field_length = len(encoded_field).to_bytes(
                FIELD_LENGTH_BYTES_AMOUNT, "big", signed=False
            )
            result += encoded_field_length
            result += encoded_field

        if add_checksum: 
            result = Protocol._add_checksum(result)

        return result
    
    @staticmethod
    def add_to_batch(current_batch: bytes, row: List[str]) -> bytes:
        encoded_row = Protocol.encode(row)
        encoded_row_length = len(encoded_row).to_bytes(
            FIELD_LENGTH_BYTES_AMOUNT, "big", signed=False
        )

        return current_batch + encoded_row_length + encoded_row

    @staticmethod
    def insert_before_batch(current_batch: bytes, row: List[str]) -> bytes:
        encoded_row = Protocol.encode(row)
        encoded_row_length = len(encoded_row).to_bytes(
            FIELD_LENGTH_BYTES_AMOUNT, "big", signed=False
        )

        return encoded_row_length + encoded_row + current_batch

    @staticmethod
    def decode_batch(message: bytes, has_checksum = False) -> list[list[str]]:
        res = []
        while len(message) > 0:
            row_length = int.from_bytes(message[:4], "big", signed=False)
            message = message[4:]

            row = message[:row_length]
            res.append(Protocol.decode(row))

            message = message[row_length:]

        return res

    @staticmethod
    def decode(message: bytes, has_checksum=False) -> List[str]:
        if has_checksum: 
            message = Protocol.__remove_and_validate_checksum(message)

        result = []
        while len(message) > 0:
            field_length = int.from_bytes(message[:4], "big", signed=False)
            message = message[4:]

            field = message[:field_length]
            field = field.decode("utf-8")
            result.append(field)
            message = message[field_length:]

        return result


class TestProtocol(unittest.TestCase):
    def test_encode(self):
        # Test data
        row = ["Hello", "World"]
        expected_bytes = b"\x00\x00\x00\x05Hello\x00\x00\x00\x05World"

        # Call the encode method
        encoded = Protocol.encode(row)

        # Assert the result
        self.assertEqual(encoded, expected_bytes)

    def test_decode(self):
        # Test data
        message = b"\x00\x00\x00\x05Hello\x00\x00\x00\x05World"
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

    def test_add_to_batch(self):
        # Test adding to an empty batch
        current_batch = b""
        new_row = ["Hello", "World"]
        expected_batch = b"\x00\x00\x00\x12\x00\x00\x00\x05Hello\x00\x00\x00\x05World"

        result = Protocol.add_to_batch(current_batch, new_row)
        self.assertEqual(result, expected_batch)

    def test_add_to_batch_with_existing_batch(self):
        # Initial batch (already contains one row)
        current_batch = b"\x00\x00\x00\x12\x00\x00\x00\x05Hello\x00\x00\x00\x05World"
        new_row = ["Test", "Data"]
        expected_batch = (
            b"\x00\x00\x00\x12\x00\x00\x00\x05Hello\x00\x00\x00\x05World"
            b"\x00\x00\x00\x10\x00\x00\x00\x04Test\x00\x00\x00\x04Data"
        )

        # Call add_to_batch method
        result = Protocol.add_to_batch(current_batch, new_row)

        # Assert the result matches expected batch
        self.assertEqual(result, expected_batch)

    def test_decode_batch(self):
        # Test data
        batch = (
            b"\x00\x00\x00\x12\x00\x00\x00\x05Hello\x00\x00\x00\x05World"
            b"\x00\x00\x00\x10\x00\x00\x00\x04Test\x00\x00\x00\x04Data"
        )
        expected_decoded = [["Hello", "World"], ["Test", "Data"]]

        # Call the decode_batch method
        decoded = Protocol.decode_batch(batch)

        # Assert the result
        self.assertEqual(decoded, expected_decoded)


if __name__ == "__main__":
    unittest.main()
