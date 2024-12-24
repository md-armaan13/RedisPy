import struct

from crc import append_crc64, write_crc64_placeholder , CRC64FileWrapper


def encode_length_for_write(length: int) -> bytes:
    """
    Encode a length according to the RDB size-encoding rules for writing.
    """
    if length <= 63:
        # 00xxxxxx
        return struct.pack('B', length)
    elif length <= 16383:
        # 01xxxxxx xxxxxxxx
        first_byte = 0b01000000 | ((length >> 8) & 0x3F)
        second_byte = length & 0xFF
        return struct.pack('BB', first_byte, second_byte)
    else:
        # 10xxxxxx [4-byte big-endian]
        return struct.pack('B', 0b10000000) + struct.pack('>I', length)

def encode_string_for_write(s: str) -> bytes:
    """
    Encode a string for writing to the RDB file.
    """
    s_bytes = s.encode('utf-8')
    length = len(s_bytes)
    return encode_length_for_write(length) + s_bytes

def encode_expire_ms_for_write(timestamp_ms: int) -> bytes:
    """
    Encode the expiration timestamp in milliseconds.
    """
    return b'\xFC' + struct.pack('<Q', timestamp_ms)

def encode_expire_s_for_write(timestamp_s: int) -> bytes:
    """
    Encode the expiration timestamp in seconds.
    """
    return b'\xFD' + struct.pack('<I', timestamp_s)

def encode_list_for_write(lst: list) -> bytes:
    """
    Encode a Python list into RDB list encoding.
    """
    encoded = b''
    # Encode list size
    encoded += encode_length_for_write(len(lst))
    # Encode each list element as a string
    for item in lst:
        encoded += encode_string_for_write(item)
    return encoded

def encode_set_for_write(s: set) -> bytes:
    """
    Encode a Python set into RDB set encoding.
    """
    encoded = b''
    # Value type flag for sets
    encoded += struct.pack('B', 0x02)
    # Encode set size
    encoded += encode_length_for_write(len(s))
    # Encode each set member as a string
    for member in s:
        encoded += encode_string_for_write(member)
    return encoded

def write_rdb(filename: str, data_store: dict, expiry_store: dict):
    """
    Writes the in-memory data_store and expiry_store to an RDB file.
    """
    try:
        with open(filename, 'wb') as raw_file:

            file = CRC64FileWrapper(raw_file)
            # Step 1: Write Header
            header = b'REDIS0011'
            file.write(header)

            # Step 2: Write Metadata (optional)
            # Example: write "redis-ver" metadata
            # OpCode FA
            file.write(b'\xFA')
            # Metadata name
            meta_name = "redis-ver"
            file.write(encode_string_for_write(meta_name))
            # Metadata value
            meta_value = "6.0.16"
            file.write(encode_string_for_write(meta_value))

            # Step 3: Write Database Section
            # Start with DB 0
            file.write(b'\xFE')
            file.write(encode_length_for_write(0))  # DB index 0

            # Write RESIZEDB opcode FB
            file.write(b'\xFB')
            main_ht_size = len(data_store)
            expire_ht_size = len(expiry_store)
            file.write(encode_length_for_write(main_ht_size))
            file.write(encode_length_for_write(expire_ht_size))

            # Write key-value pairs
            for key, value in data_store.items():
                # Check for expiration
                if key in expiry_store:
                    expire_timestamp = expiry_store[key]
                    # Decide whether to write FD or FC based on timestamp
                    # For simplicity, use FC (milliseconds)
                    print("Found Expiry" , expire_timestamp)
                    file.write(encode_expire_ms_for_write(expire_timestamp))
                # Value type: 0x00 for string
                if isinstance(value, str):
                    # Value type: 0x00 for string
                    file.write(struct.pack('B', 0x00))
                    file.write(encode_string_for_write(key))
                    file.write(encode_string_for_write(value))
                elif isinstance(value, list):
                    # Value type: 0x01 for list
                    file.write(struct.pack('B', 0x01))
                    file.write(encode_string_for_write(key))
                    file.write(encode_list_for_write(value))
                elif isinstance(value, set):
                    # Value type: 0x02 for set
                    file.write(struct.pack('B', 0x02))
                    file.write(encode_string_for_write(key))
                    file.write(encode_set_for_write(value))
                else:
                    print(f"Unsupported data type for key: {key}")
                    continue  # Skip unsupported types

            # Step 4: End of File
            file.write(b'\xFF')

            # Placeholder for CRC64 checksum (zeroed)
            # write_crc64_placeholder(file)
        print(filename)
        append_crc64(filename)
        print(f"RDB file written successfully: {filename}")
    except Exception as e:
        print(f"Error writing RDB file: {e}")
        raise
