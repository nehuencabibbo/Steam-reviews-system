import logging
import unittest

from typing import *

FIELD_LENGTH_BYTES_AMOUNT = 4


class Protocol:
    @staticmethod
    def encode(row: List[str]) -> bytes:
        result = b""
        for field in row:
            encoded_field = field.encode("utf-8")
            encoded_field_length = len(encoded_field).to_bytes(
                FIELD_LENGTH_BYTES_AMOUNT, "big", signed=False
            )
            result += encoded_field_length
            result += encoded_field

        return result

    @staticmethod
    def add_to_batch(current_batch: bytes, row: List[str]) -> bytes:
        encoded_row = Protocol.encode(row)
        encoded_row_length = len(encoded_row).to_bytes(
            FIELD_LENGTH_BYTES_AMOUNT, "big", signed=False
        )

        return current_batch + encoded_row_length + encoded_row

    @staticmethod
    def decode_batch(message: bytes) -> list[list[str]]:
        res = []
        while len(message) > 0:
            row_length = int.from_bytes(message[:4], "big", signed=False)
            message = message[4:]

            row = message[:row_length]
            res.append(Protocol.decode(row))

            message = message[row_length:]

        return res

    @staticmethod
    def decode(message: bytes) -> List[str]:
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
