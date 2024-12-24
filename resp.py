import asyncio
from constants import DataType, Responses, Errors
from typing import Any, List, Optional, Tuple, Union

class RESPEncoder:
    @staticmethod
    def encode_simple_string(s: str) -> bytes:
        return f"{DataType.SIMPLE_STRING.decode()}{s}{DataType.TERMINATOR.decode()}".encode()

    @staticmethod
    def encode_error(message: str) -> bytes:
        return f"{DataType.SIMPLE_ERROR.decode()}{message}{DataType.TERMINATOR.decode()}".encode()

    @staticmethod
    def encode_integer(value: int) -> bytes:
        return f"{DataType.INTEGER.decode()}{value}{DataType.TERMINATOR.decode()}".encode()

    @staticmethod
    def encode_bulk_string(s: Optional[str]) -> bytes:
        if s is None:
            return DataType.NULL_BULK_STRING
        encoded = s.encode()
        return f"{DataType.BULK_STRING.decode()}{len(encoded)}{DataType.TERMINATOR.decode()}".encode() + encoded + DataType.TERMINATOR

    @staticmethod
    def encode_array(arr: Optional[List[Any]]) -> bytes:
        if arr is None:
            return DataType.NULL_ARRAY
        encoded_elements = b"".join([RESPEncoder.encode(data) for data in arr])
        return f"{DataType.ARRAY.decode()}{len(arr)}{DataType.TERMINATOR.decode()}".encode() + encoded_elements

    @staticmethod
    def encode(data: Any) -> bytes:
        if isinstance(data, str):
            return RESPEncoder.encode_bulk_string(data)
        elif isinstance(data, bytes):
            return RESPEncoder.encode_bulk_string(data.decode())
        elif isinstance(data, int):
            return RESPEncoder.encode_integer(data)
        elif isinstance(data, list):
            return RESPEncoder.encode_array(data)
        elif data is None:
            return RESPEncoder.encode_bulk_string(None)
        else:
            raise TypeError(f"Unsupported type for RESP encoding: {type(data)}")

class RESPDecoder:
    def __init__(self):
        self.buffer = b""

    def feed(self, data: bytes):
        self.buffer += data

    async def decode(self) -> Optional[Union[str, List[Any], int, dict]]:
        if not self.buffer:
            return None

        prefix = chr(self.buffer[0])

        if prefix == DataType.ARRAY.decode():
            return self._decode_array()
        elif prefix == DataType.BULK_STRING.decode():
            return self._decode_bulk_string()
        elif prefix == DataType.SIMPLE_STRING.decode():
            return self._decode_simple_string()
        elif prefix == DataType.SIMPLE_ERROR.decode():
            return self._decode_error()
        elif prefix == DataType.INTEGER.decode():
            return self._decode_integer()
        else:
            # Attempt to parse inline command
            return self._decode_inline()

    def _read_line(self) -> Optional[str]:
        newline_index = self.buffer.find(DataType.TERMINATOR)
        if newline_index == -1:
            return None
        line = self.buffer[:newline_index]
        self.buffer = self.buffer[newline_index + len(DataType.TERMINATOR):]
        return line.decode()

    def _decode_simple_string(self) -> Optional[str]:
        line = self._read_line()
        return line

    def _decode_error(self) -> Optional[dict]:
        line = self._read_line()
        return {"error": line}

    def _decode_integer(self) -> Optional[int]:
        line = self._read_line()
        if line is None:
            return None
        try:
            return int(line)
        except ValueError:
            return None

    def _decode_bulk_string(self) -> Optional[str]:
        line = self._read_line()
        if line is None:
            return None
        length = int(line)
        if length == -1:
            return None
        if len(self.buffer) < length + len(DataType.TERMINATOR):
            return None  # Wait for more data
        data = self.buffer[:length]
        self.buffer = self.buffer[length + len(DataType.TERMINATOR):]
        return data.decode()

    def _decode_array(self) -> Optional[List[Any]]:
        line = self._read_line()
        if line is None:
            return None
        num_elements = int(line)
        if num_elements == -1:
            return None
        elements = []
        for _ in range(num_elements):
            if not self.buffer:
                return None
            prefix = chr(self.buffer[0])
            if prefix == DataType.BULK_STRING.decode():
                elem = self._decode_bulk_string()
                if elem is None and line != "-1":
                    return None
                elements.append(elem)
            elif prefix == DataType.SIMPLE_STRING.decode():
                elem = self._decode_simple_string()
                if elem is None:
                    return None
                elements.append(elem)
            elif prefix == DataType.SIMPLE_ERROR.decode():
                elem = self._decode_error()
                if elem is None:
                    return None
                elements.append(elem)
            elif prefix == DataType.INTEGER.decode():
                elem = self._decode_integer()
                if elem is None:
                    return None
                elements.append(elem)
            elif prefix == DataType.ARRAY.decode():
                elem = self._decode_array()
                if elem is None:
                    return None
                elements.append(elem)
            else:
                raise ValueError(f"Unsupported RESP type in array: {prefix}")
        return elements

    def _decode_inline(self) -> Optional[List[str]]:
        """
        Decodes inline commands (e.g., sent via telnet).
        Format: COMMAND arg1 arg2 ... argN\r\n
        """
        line = self._read_line()
        if line is None:
            return None
        if not line:
            return None
        parts = line.split()
        return parts
