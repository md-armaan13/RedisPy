# redis_like_server/constants.py

from dataclasses import dataclass

@dataclass(frozen=True)
class DataType:
    ARRAY = b"*"
    BULK_STRING = b"$"
    SIMPLE_STRING = b"+"
    SIMPLE_ERROR = b"-"
    INTEGER = b":"
    NULL = b"_"
    BOOLEAN = b"#"
    DOUBLE = b","
    BIG_NUMBER = b"("
    BULK_ERROR = b"!"
    VERBATIM_STRING = b"="
    MAP = b"%"
    ATTRIBUTES = b"`"
    SET = b"~"
    PUSH = b">"

@dataclass(frozen=True)
class Constants:
    NULL_BULK_STRING = b"$-1\r\n"
    TERMINATOR = b"\r\n"
    EMPTY_BYTE = b""
    SPACE_BYTE = b" "
    PONG = b"PONG\r\n"
    OK = b"OK\r\n"
    INVALID_COMMAND = b"ERR Invalid Command\r\n"
    NOPROTO_ERROR = b"-NOPROTO sorry, this protocol version is not supported.\r\n"
    UNKNOWN_COMMAND_ERROR = b"-ERR unknown command '%s'\r\n"
    UNKNOWN_SUBCOMMAND_ERROR = b"-ERR unknown subcommand '%s' for 'CONFIG' command\r\n"
    SYNTAX_ERROR = b"-ERR syntax error\r\n"

@dataclass(frozen=True)
class Command:
    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"
    CONFIG = "CONFIG"
    QUIT = "QUIT"
    EXIT = "EXIT"

