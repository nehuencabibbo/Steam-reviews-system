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

    @staticmethod
    def insert_total_length(message: bytes) -> bytes:
        return (
            len(message).to_bytes(FIELD_LENGTH_BYTES_AMOUNT, "big", signed=False)
            + message
        )

    @staticmethod
    def decode_length(message: bytes) -> int:
        return int.from_bytes(message, "big", signed=False)

    @staticmethod
    def get_message_type(message: bytes) -> Tuple[str, bytes]:
        # In order to avoid the decoding+encoding in the client_handler, we only extract and decode this part
        msg_type = Protocol.decode_batch(message[:9])[0][0]
        return msg_type, message[9:]

    @staticmethod
    def get_session_id(message: bytes) -> Tuple[str, bytes]:
        # In order to avoid the decoding+encoding in the client_handler, we only extract and decode this part
        session_id_length = int.from_bytes(message[4:8], "big", signed=False)
        session_id = message[8 : 8 + session_id_length].decode("utf8")
        return session_id, message[8 + session_id_length :]

    @staticmethod
    def get_row_length(message: bytes) -> int:
        return Protocol.decode_length(message[:FIELD_LENGTH_BYTES_AMOUNT])

    @staticmethod
    def get_first_row(message: bytes) -> Tuple[int, int]:
        length = (
            Protocol.decode_length(message[:FIELD_LENGTH_BYTES_AMOUNT])
            + FIELD_LENGTH_BYTES_AMOUNT
        )
        return message[FIELD_LENGTH_BYTES_AMOUNT:length], length

    @staticmethod
    def get_row_field(field: int, encoded_row: bytes) -> int:
        return Protocol.decode(encoded_row)[
            field
        ]  # [:4] in order to avoid the row total length


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

    def test_for_log(self):
        data = ['/tmp/006b8b4567/platform_count.csv', 'WINDOWS,722', 'MAC,133', 'LINUX,85']
        msg_ids = ['006b8b4567', '729,L', '773,L', '770,W', '771,W', '772,W', '773,W', '775,W', '776,W', '729,L', '773,L', '773,M', '776,M', '729,L', '773,L']

        encoded_data = Protocol.encode(data, add_checksum=True)
        encoded_msg_ids = Protocol.encode(msg_ids, add_checksum=True)

        print(encoded_data)
        print("----------------------------------------")
        print(encoded_msg_ids)

if __name__ == "__main__":
    unittest.main()
