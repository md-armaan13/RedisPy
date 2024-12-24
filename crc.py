import struct
import crcmod

crc64 =crcmod.predefined.mkPredefinedCrcFun('crc-64')
class CRC64FileWrapper:
    def __init__(self, file_obj):
        self.file = file_obj
        # Initialize the CRC64 function using a predefined CRC64 polynomial
        self.crc64_func = crcmod.predefined.mkPredefinedCrcFun('crc-64')
        self.crc = 0  # Initial CRC value

    def write(self, data: bytes):
        """
        Writes data to the file and updates the CRC64 checksum.
        """
        self.crc = self.crc64_func(data, self.crc)
        self.file.write(data)

    def get_crc64(self) -> int:
        """
        Returns the final CRC64 checksum.
        """
        return self.crc


def write_crc64_placeholder(file_wrapper: CRC64FileWrapper):
    """
    Writes the CRC64 checksum at the end of the RDB file.
    """
    checksum = file_wrapper.get_crc64()
    print(f"Checksum {checksum}")
    # Pack the checksum as an 8-byte big-endian unsigned long
    checksum_bytes = struct.pack('>Q', checksum)

    file_wrapper.write(checksum_bytes)
def calculate_crc64(file_path):
    """Calculate the CRC64 checksum of a file."""
    with open(file_path, 'rb') as f:
        file_content = f.read()
    checksum = crc64(file_content)
    print(checksum)
    return checksum # Return as a 16-character hexadecimal string

def append_crc64(file_path):
    """Calculate and append the CRC64 checksum to the file."""
    checksum = calculate_crc64(file_path)
    with open(file_path, 'ab') as f:
        checksum_bytes = struct.pack('>Q', checksum)
        f.write(checksum_bytes)

def verify_crc(file_path):
    """Verify the CRC64 checksum of the file."""
    with open(file_path, 'rb') as f:
        # Read the entire file content
        file_content = f.read()

    # Extract the checksum (last 8 bytes) and the original content
    if len(file_content) < 8:
        raise ValueError("File is too small to contain a CRC64 checksum.")

    checksum_from_file = struct.unpack('>Q', file_content[-8:])[0]
    original_content = file_content[:-8]

    # Recalculate the checksum
    recalculated_checksum = crc64(original_content)

    # Compare the checksums
    if checksum_from_file == recalculated_checksum:
        print("CRC64 checksum verification succeeded.")
        return True
    else:
        print("CRC64 checksum verification failed.")
        return False