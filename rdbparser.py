import io
import struct
import time
import crcmod
import os
from crc import CRC64FileWrapper, verify_crc, write_crc64_placeholder  # Ensure correct import

def read_byte(file) -> int:
    """
    Reads a single byte from the file and returns its integer value.
    """
    byte = file.read(1)
    if not byte:
        raise EOFError("Unexpected end of file while reading a byte.")
    return byte[0]

def read_length(file) -> int:
    """
    Decodes the length encoding as per RDB specifications.
    """
    first_byte = read_byte(file)
    encoding = (first_byte & 0xC0) >> 6  # Top 2 bits
    if encoding == 0:  # 00xxxxxx, single byte length
        return first_byte
    elif encoding == 1:  # 01xxxxxx xxxxxxxx, two bytes length
        second_byte = read_byte(file)
        length = ((first_byte & 0x3F) << 8) | second_byte
        return length
    elif encoding == 2:  # 10xxxxxx [4-byte big-endian], five bytes total
        # Reserved in RDB for sizes > 16383
        # Here we can skip or handle as needed
        # For simplicity, read the next 4 bytes as big-endian unsigned int
        length_bytes = file.read(4)
        if len(length_bytes) != 4:
            raise EOFError("Unexpected end of file while reading extended length.")
        length = struct.unpack('>I', length_bytes)[0]
        return length
    else:
        raise ValueError(f"Unknown length encoding: {encoding}")

def read_string(file) -> str:
    """
    Reads a length-prefixed string from the file.
    """
    length = read_length(file)
    if length == -1:
        return None  # Null Bulk String
    string_bytes = file.read(length)
    if len(string_bytes) != length:
        raise EOFError("Unexpected end of file while reading a string.")
    return string_bytes.decode('utf-8')

def read_expire(file , expire_opcode) -> int:
    """
    Reads the expiration timestamp from the file.
    Returns the timestamp in milliseconds.
    """

    if expire_opcode == 0xFC:  # Expires in milliseconds
        timestamp_bytes = file.read(8)
        if len(timestamp_bytes) != 8:
            raise EOFError("Unexpected end of file while reading expiration timestamp.")
        timestamp_ms = struct.unpack('<Q', timestamp_bytes)[0]
        return timestamp_ms
    elif expire_opcode == 0xFD:  # Expires in seconds
        timestamp_bytes = file.read(4)
        if len(timestamp_bytes) != 4:
            raise EOFError("Unexpected end of file while reading expiration timestamp.")
        timestamp_s = struct.unpack('<I', timestamp_bytes)[0]
        return timestamp_s * 1000  # Convert to milliseconds
    else:
        # No expiration; the byte read is actually a value type opcode
        return None, expire_opcode  # Returning the opcode to handle it outside

def verify_crc64(file, expected_checksum: int):
    """
    Verifies the CRC64 checksum of the file.
    """
    # Move to the beginning of the file
    file.seek(0)
    # Read all data except the last 8 bytes (checksum)
    data = file.read(-8)
    # Read the stored checksum
    stored_checksum_bytes = file.read(8)
    if len(stored_checksum_bytes) != 8:
        raise EOFError("Unexpected end of file while reading CRC64 checksum.")
    stored_checksum = struct.unpack('>Q', stored_checksum_bytes)[0]

    # Compute CRC64
    crc64_func = crcmod.predefined.mkPredefinedCrcFun('crc-64')
    computed_checksum = crc64_func(data)

    if computed_checksum != stored_checksum:
        raise ValueError("CRC64 checksum does not match. RDB file may be corrupted.")

def parse_rdb(filename: str, data_store: dict, expiry_store: dict):
    """
    Parses the RDB file and populates data_store and expiry_store.
    """
    try:
        with open(filename, 'rb') as file:
            # Step 1: Read the entire file into memory
            file_content = file.read()
            if len(file_content) < 17:
                raise ValueError("RDB file is too short to be valid.")

            # Step 2: Separate RDB data and stored checksum
            rdb_data = file_content[:-8]

            # Step 5: Parse RDB data
            rdb_buffer = io.BytesIO(rdb_data)

            # Begin parsing
            # Step 1: Verify Header
            header = rdb_buffer.read(9)
            if header != b'REDIS0011':
                raise ValueError("Invalid RDB file header.")
            print("RDB header verified.")

            while True:
                try:
                    opcode = read_byte(rdb_buffer)
                    if opcode == 0xFA:  # Metadata section
                        meta_key = read_string(rdb_buffer)
                        meta_value = read_string(rdb_buffer)
                        print(f"Metadata - {meta_key}: {meta_value}")

                    elif opcode == 0xFE:  # Database selector
                        db_index = read_length(rdb_buffer)
                        if db_index != 0:
                            raise ValueError("Only database 0 is supported.")
                        print(f"Selected Database: {db_index}")

                    elif opcode == 0xFB:  # RESIZEDB
                        main_ht_size = read_length(rdb_buffer)
                        expire_ht_size = read_length(rdb_buffer)
                        print(f"Main Hash Table Size: {main_ht_size}, Expire Hash Table Size: {expire_ht_size}")

                        # Iterate through main_ht_size key-value pairs
                        for _ in range(main_ht_size):
                            # Peek the next byte to check for expiration
                            next_byte = rdb_buffer.read(1)
                            if not next_byte:
                                raise EOFError("Unexpected end of file while reading key-value pairs.")
                            next_opcode = next_byte[0]
                            # if next_opcode in (0xFC, 0xFD):
                            #     # Read expiration
                            #     expire_timestamp = read_expire(rdb_buffer)
                            #     print(f"found Expiry {expire_timestamp}")
                            #     # After reading expiration, read the value type opcode
                            #     value_type = read_byte(rdb_buffer)
                            if next_opcode == 0xFC :
                                expire_timestamp = read_expire(rdb_buffer , 0xFC)
                                value_type = read_byte(rdb_buffer)
                            elif next_opcode == 0xFD :
                                expire_timestamp = read_expire(rdb_buffer , 0xFD)
                                value_type = read_byte(rdb_buffer)
                            else:
                                # No expiration; the byte read is actually the value type opcode
                                value_type = next_opcode
                                expire_timestamp = None

                            # Read key
                            key = read_string(rdb_buffer)

                            # Read value based on value type
                            if value_type == 0x00:  # String
                                value = read_string(rdb_buffer)
                                data_store[key] = value
                            elif value_type == 0x01:  # List
                                list_size = read_length(rdb_buffer)
                                value = [read_string(rdb_buffer) for _ in range(list_size)]
                                data_store[key] = value
                            elif value_type == 0x02:  # Set
                                set_size = read_length(rdb_buffer)
                                value = set([read_string(rdb_buffer) for _ in range(set_size)])
                                data_store[key] = value
                            else:
                                raise ValueError(f"Unknown value type opcode: {value_type}")

                            # Store expiration if present
                            if expire_timestamp is not None:
                                current_time = int(time.time() * 1000)
                                if not expire_timestamp < current_time :
                                    expiry_store[key] = expire_timestamp

                            print(f"Loaded Key: {key}, Type: {value_type}, Expire: {expire_timestamp}, Value: {value}")

                    elif opcode == 0xFF:  # End of file
                        print("Reached End of RDB file.")
                        break  # Parsing complete

                    else:
                        raise ValueError(f"Unknown opcode encountered: {opcode}")

                except EOFError:
                    print("Reached unexpected end of RDB file.")
                    break
                except Exception as parse_e:
                    print(f"Error parsing RDB file: {parse_e}")
                    break
        print("RDB file parsed successfully.")
    except Exception as parse_e:
        print(f"Error parsing RDB file: {parse_e}")
