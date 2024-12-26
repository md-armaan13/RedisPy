import fnmatch
import socket
import threading
import time
import random
import argparse
import os
from rdbparser import parse_rdb
from save_rdb import write_rdb
from utils import random_id

data_store = {}
expiry_store = {}
data_store_lock = threading.Lock()
expiry_store_lock = threading.Lock()


config = {
    "dir": "/tmp/redis-data",
    "dbfilename": "dump.rdb",
    "role" : "master",
    "master_host": None,
    "master_port": None,
    "master_replid" : None,
    "master_repl_offset" : 0
}

def active_expiration():
    """
    Periodically checks a random subset of keys with TTL and removes expired ones.
    """
    while True:
        current_time = int(time.time() * 1000)

        # Random sampling outside the lock
        with expiry_store_lock:
            keys_with_ttl = list(expiry_store.keys())

        if len(keys_with_ttl) > 0:
            keys_to_check = random.sample(keys_with_ttl, min(20, len(keys_with_ttl)))

            # Delete expired keys within the lock
            for key in keys_to_check:
                with expiry_store_lock:
                    if key in expiry_store and expiry_store[key] <= current_time:
                        with data_store_lock:
                            data_store.pop(key, None)
                        expiry_store.pop(key, None)
                        print(f"{key} deleted")

        # Wait before the next active expiration cycle
        time.sleep(0.1)  # 100ms interval


def parse_redis_protocol(data):
    """
    Parses the Redis protocol and returns the command and arguments.
    Example input: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    Output: ["ECHO", "hey"]
    """
    lines = data.split("\r\n")
    if not lines or lines[0][0] != '*':
        return None  # Invalid format

    num_args = int(lines[0][1:])
    args = []
    idx = 1
    while len(args) < num_args:
        if lines[idx][0] == '$':
            args.append(lines[idx + 1])
            idx += 2
        else:
            idx += 1
    return args


def handle_set_command(conn, args):
    """
    Handles the SET command with optional PX argument for expiration.
    """
    if len(args) >= 2:
        key = args[0]
        value = args[1]
        expiry_time = None

        if len(args) > 2:
            if args[2].upper() != "PX":
                conn.sendall(b"-ERR Invalid arguments for SET\r\n")
                return
            if len(args) > 3:
                try:
                    expiry_time = int(args[3])
                    expiration = int(time.time() * 1000) + expiry_time

                    # Set expiration with finer-grained locks
                    with expiry_store_lock:
                        expiry_store[key] = expiration

                except ValueError:
                    conn.sendall(b"-ERR PX value must be an integer\r\n")
                    return
            else:
                conn.sendall(b"-ERR PX value missing\r\n")
                return
        elif key in expiry_store :
            expiry_store.pop(key,None)
        # Set the key-value pair in the data store
        with data_store_lock:
            data_store[key] = value
        conn.sendall(b"+OK\r\n")
    else:
        conn.sendall(b"-ERR Wrong number of arguments for SET\r\n")

def handle_get_command(conn, args):
    """
    Handles the GET command with lazy expiration.
    """
    if len(args) == 1:
        key = args[0]
        current_time = int(time.time() * 1000)

        # Check expiration with locks
        with expiry_store_lock:
            if key in expiry_store and expiry_store[key] <= current_time:
                # Key has expired
                with data_store_lock:
                    data_store.pop(key, None)
                expiry_store.pop(key, None)
                conn.sendall(b"$-1\r\n")  # Null bulk string
                return

        # Retrieve the key from the data store
        with data_store_lock:
            value = data_store.get(key)
            if value is not None :
                response = f"${len(value)}\r\n{value}\r\n"
                conn.sendall(response.encode())
            else:
                conn.sendall(b"$-1\r\n")  # Null bulk string
    else:
        conn.sendall(b"-ERR Wrong number of arguments for GET\r\n")

def handle_config_get_command(conn, args):
    """
    Handles the CONFIG GET command.
    """
    if len(args) != 2 or args[0].upper() != "GET":
        conn.sendall(b"-ERR Invalid CONFIG GET syntax\r\n")
        return

    param = args[1]
    if param not in config:
        conn.sendall(b"-ERR Unknown configuration parameter\r\n")
        return

    param_name = param
    param_value = config[param]

    # Prepare RESP array with two bulk strings
    response = f"*2\r\n${len(param_name)}\r\n{param_name}\r\n${len(param_value)}\r\n{param_value}\r\n"
    conn.sendall(response.encode())

def handle_save_command(conn):
    """
    Handles the SAVE command by writing the in-memory data to an RDB file.
    """
    try:
        # Acquire both locks to ensure data consistency
        with data_store_lock, expiry_store_lock:
            # Construct the full path for the RDB file
            rdb_path = f"{config['dir']}/{config['dbfilename']}"

            # Ensure the directory exists
            import os
            os.makedirs(config['dir'], exist_ok=True)

            # Write the RDB file
            write_rdb(rdb_path, data_store, expiry_store)

        # Send success response to the client
        conn.sendall(b"+OK\r\n")
        print(f"RDB file saved successfully at {rdb_path}")
    except Exception as e:
        # Send error response to the client
        conn.sendall(b"-ERR Failed to save RDB file\r\n")
        print(f"Error saving RDB file: {e}")

def handle_keys_command(conn, args):
    """
    Handles the KEYS command by returning all keys matching the given pattern.
    """
    if len(args) != 1:
        conn.sendall(b"-ERR Wrong number of arguments for KEYS\r\n")
        return

    pattern = args[0]

    with data_store_lock:
        keys = list(data_store.keys())

    # Match keys using fnmatch
    matching_keys = [key for key in keys if fnmatch.fnmatch(key, pattern)]

    # Build RESP array
    response = f"*{len(matching_keys)}\r\n"
    for key in matching_keys:
        response += f"${len(key)}\r\n{key}\r\n"

    conn.sendall(response.encode())

def handle_info_command(conn , args) :
    """
    Handles the Info command by returning returns information and statistics about a Redis server.
    """
    if len(args) != 1:
        conn.sendall(b"-ERR Wrong number of arguments for INFO\r\n")
        return

    section = args[0].lower()

    if section == "replication":

        role = config["role"]
        master_replid = config["master_replid"]
        master_repl_offset = config["master_repl_offset"]

        response_string = f"# Replication\nrole:{role}\nmaster_replid:{master_replid}\nmaster_repl_offset:{master_repl_offset}\n"

        # RESP Bulk String format: $<length>\r\n<response_string>
        response = f"${len(response_string)}\r\n{response_string}\r\n"
        conn.sendall(response.encode())
    else:
        # If other sections are requested, respond with an empty bulk string
        conn.sendall(b"-ERR Wrong arguments for 'info' command\r\n")

def handle_replconf_command(conn, args):
    """
    Handles the REPLCONF command by responding with +OK\r\n
    """
    if len(args) != 2:
        conn.sendall(b"-ERR Wrong number of arguments for REPLCONF\r\n")
        return

    key = args[0]
    value = args[1]
    print(f"Received REPLCONF command: {key} {value}")

    # For this stage, simply respond with +OK\r\n
    conn.sendall(b"+OK\r\n")
def handle_psync_command(conn, args):
    """
    Handles the PSYNC command by responding with +FULLRESYNC <REPL_ID> 0\r\n
    """
    if len(args) != 2:
        conn.sendall(b"-ERR Wrong number of arguments for PSYNC\r\n")
        return

    repl_id = args[0]
    repl_offset = args[1]

    # For this stage, respond with +FULLRESYNC <REPL_ID> 0\r\n
    # If repl_id is "?", and repl_offset is "-1", perform a full resync
    master_replid = config["master_replid"]
    if repl_id == "?" and repl_offset == "-1":
        response = f"+FULLRESYNC {master_replid} 0\r\n"
        conn.sendall(response.encode())
        print(f"Handled PSYNC command with repl_id={repl_id}, repl_offset={repl_offset}")

        rdb_path = os.path.join(config['dir'], config['dbfilename'])

        if os.path.exists(rdb_path):
            try:
                with open(rdb_path, 'rb') as rdb_file:
                    rdb_content = rdb_file.read()
                rdb_length = len(rdb_content)
                # Prepare the RDB file in the specified format
                rdb_response = f"${rdb_length}\r\n".encode() + rdb_content
                conn.sendall(rdb_response)
                print(f"Sent RDB file ({rdb_length} bytes) to replica.")
            except Exception as e:
                conn.sendall(b"-ERR Failed to read RDB file\r\n")
                print(f"Error reading RDB file: {e}")
                
    else:
        # For simplicity, only handle the initial PSYNC ? -1
        conn.sendall(b"-ERR Unsupported PSYNC arguments\r\n")
        print(f"Received unsupported PSYNC arguments: repl_id={repl_id}, repl_offset={repl_offset}")


def replica_handshake():
    """
    Handles the handshake process from the replica's side by connecting to the master,
    sending PING, and then sending two REPLCONF commands.
    """
    master_host = config["master_host"]
    master_port = int(config["master_port"])
    replica_port = config["port"]

    try:
        with socket.create_connection((master_host, master_port)) as sock:
            print(f"Connected to master at {master_host}:{master_port} for replication handshake.")

            # === Part 1: Send PING ===
            ping_command = "*1\r\n$4\r\nPING\r\n"
            sock.sendall(ping_command.encode())
            print("Sent PING to master.")

            # Receive PONG response
            response = sock.recv(1024).decode()
            print(f"Received response from master: {response.strip()}")

            # === Part 2: Send REPLCONF listening-port <PORT> ===
            replconf_listening_port = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(replica_port))}\r\n{replica_port}\r\n"
            sock.sendall(replconf_listening_port.encode())
            print(f"Sent REPLCONF listening-port {replica_port} to master.")

            # Receive +OK response
            response = sock.recv(1024).decode()
            print(f"Received response from master: {response.strip()}")

            # === Part 2: Send REPLCONF capa psync2 ===
            replconf_capa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
            sock.sendall(replconf_capa.encode())
            print("Sent REPLCONF capa psync2 to master.")

            # Receive +OK response
            response = sock.recv(1024).decode()
            print(f"Received response from master: {response.strip()}")

            # === Part 3: Send PSYNC ? -1 ===
            psync_command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
            sock.sendall(psync_command.encode())
            print("Sent PSYNC ? -1 to master.")

            # Receive +FULLRESYNC <REPL_ID> 0\r\n response
            response = sock.recv(1024).decode()
            print(f"Received response from master: {response.strip()}")

            # For this stage, we can keep the connection open or close it.
            # Here, we'll keep it open to simulate an ongoing replication connection.
            # while True:
            #     time.sleep(1)  # Keep the thread alive

    except Exception as e:
        print(f"Failed to connect to master {master_host}:{master_port} - {e}")

def handle_client(conn, addr):
    with conn:
        while True:
            try:
                data = conn.recv(1024)  # Receive up to 1024 bytes
                if not data:
                    break  # Client disconnected

                # Parse the Redis protocol
                command_parts = parse_redis_protocol(data.decode())
                if not command_parts:
                    conn.sendall(b"-ERR Invalid command\r\n")
                    continue

                # Handle commands
                command = command_parts[0].upper()
                args = command_parts[1:]

                if command == "PING":
                    conn.sendall(b"+PONG\r\n")
                elif command == "ECHO" and len(args) == 1:
                    response = f"${len(args[0])}\r\n{args[0]}\r\n"
                    conn.sendall(response.encode())
                elif command == "SET":
                    handle_set_command(conn, args)
                elif command == "GET":
                    handle_get_command(conn, args)
                elif command == "CONFIG":
                    handle_config_get_command(conn, args)
                elif command == "SAVE" :
                    handle_save_command(conn)
                elif command == "KEYS":
                    handle_keys_command(conn, args)
                elif command == "INFO" :
                    handle_info_command(conn , args)
                elif command == "REPLCONF":
                    handle_replconf_command(conn, args)
                elif command == "PSYNC":
                    handle_psync_command(conn, args)
                elif command in ("QUIT", "EXIT"):
                    conn.sendall(b"+OK\r\n")
                    break  # Exit the loop
                else:
                    conn.sendall(b"-ERR Unknown command\r\n")

            except ConnectionResetError:
                print(f"Connection reset by client {addr}")
                break
            except Exception as e:
                print(f"Error handling client {addr}: {e}")
                break


def main():

    try :
        global config

        # Parse command-line arguments
        parser = argparse.ArgumentParser(description="Simple Redis-like Server")
        parser.add_argument("--dir", type=str, default="/tmp/redis-data", help="Directory to store RDB files")
        parser.add_argument("--dbfilename", type=str, default="dump.rdb", help="Name of the RDB file")
        parser.add_argument("--port", type=str, default="6379", help="Port Number")
        parser.add_argument("--replicaof", type=str, nargs="+", default=None)

        args = parser.parse_args()

        # Update the configuration with command-line arguments
        config["dir"] = args.dir
        config["dbfilename"] = args.dbfilename
        config["port"] = args.port or 6379
        port = args.port or 6379

        if args.replicaof:
            master_host, master_port = args.replicaof[0].split(" ")
            config["role"] = "slave"
            config["master_host"] = master_host
            config["master_port"] = master_port
            print(f"Server is running in SLAVE mode. Replicating from {master_host}:{master_port}")
        else:
            config["role"] = "master"
            config["master_replid"] = random_id(40)
            print("Server is running in MASTER mode.")

        print(f"Configuration - dir: {config['dir']}, dbfilename: {config['dbfilename']}")
        print("Redis server is starting...")

        rdb_path = os.path.join(config['dir'], config['dbfilename'])
        if os.path.exists(rdb_path):
            print(f"RDB file found at {rdb_path}. Parsing...")
            parse_rdb(rdb_path, data_store, expiry_store)
        else:
            print("No RDB file found. Starting with an empty data store.")
        # Start the active expiration thread
        threading.Thread(target=active_expiration, daemon=True).start()

        if config["role"] == "slave":
            threading.Thread(target=replica_handshake, daemon=True).start()

        # Create and bind the server socket
        with socket.create_server(("localhost", int(port) )) as server_socket:
            print(f"Server is listening on localhost:{port}")

            while True:
                conn, addr = server_socket.accept()
                print(f"Client connected from {addr}")

                # Handle the connection
                threading.Thread(target=handle_client, args=(conn, addr)).start()
    except Exception as e:
            print(f"Error handling client {e}")


if __name__ == "__main__":
    main()
