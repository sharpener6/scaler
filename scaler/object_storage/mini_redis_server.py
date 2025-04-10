import asyncio
import logging
import time
from typing import Optional, Tuple, List, Dict


class MiniRedisServer:
    def __init__(self, host: str = "localhost", port: int = 6379, idle_timeout: int = 30, command_timeout: int = 5):
        self._host: str = host
        self._port: int = port
        self._idle_timeout: int = idle_timeout
        self._command_timeout: int = command_timeout
        self._store: Dict[bytes, bytes] = {}
        self._ttl: Dict[bytes, float] = {}  # key -> expiration timestamp (epoch seconds)
        self._lock: asyncio.Lock = asyncio.Lock()

    def get_address(self) -> bytes:
        return f"{self._host}:{self._port}".encode()

    async def start(self) -> None:
        server = await asyncio.start_server(self.handle_client, self._host, self._port)
        addr = server.sockets[0].getsockname()
        logging.info(f"MiniRedis listening on {addr}")

        try:
            async with server:
                await server.serve_forever()
        except asyncio.CancelledError:
            pass

        logging.info(f"MiniRedis exited")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"Accepted connection from {addr}")
        buffer = b""
        while not reader.at_eof():
            try:
                data: bytes = await asyncio.wait_for(reader.read(4096), timeout=self._idle_timeout)
                if not data:
                    break
                buffer += data

                while True:
                    result = self.parse_one_command(buffer)
                    if result is None:
                        break
                    command, buffer = result
                    logging.info(f"Received command from {addr}: {command}")
                    try:
                        response: bytes = await asyncio.wait_for(
                            self.execute_command(command), timeout=self._command_timeout
                        )
                    except asyncio.TimeoutError:
                        logging.warning(f"Command timeout from {addr}: {command}")
                        response = b"-ERR command timed out\r\n"
                    writer.write(response)
                    await asyncio.wait_for(writer.drain(), timeout=self._command_timeout)

            except (ConnectionResetError, asyncio.TimeoutError):
                logging.warning(f"Connection closed or timed out: {addr}")
                break
        writer.close()
        await writer.wait_closed()
        logging.info(f"Connection closed: {addr}")

    @staticmethod
    def parse_one_command(data: bytes) -> Optional[Tuple[List[bytes], bytes]]:
        def read_line(start: int) -> Tuple[Optional[bytes], Optional[int]]:
            end = data.find(b"\r\n", start)
            if end == -1:
                return None, None
            return data[start:end], end + 2

        pos = 0
        if pos >= len(data) or data[pos : pos + 1] != b"*":
            return None

        count_str, pos = read_line(pos + 1)
        if count_str is None or not count_str.isdigit():
            return None
        argc = int(count_str)

        args: List[bytes] = []
        for _ in range(argc):
            if pos >= len(data) or data[pos : pos + 1] != b"$":
                return None
            length_str, pos = read_line(pos + 1)
            if length_str is None or not length_str.isdigit():
                return None
            length = int(length_str)
            if pos + length + 2 > len(data):
                return None
            arg = data[pos : pos + length]
            args.append(arg)
            pos += length + 2  # skip \r\n

        return args, data[pos:]

    async def execute_command(self, command: List[bytes]) -> bytes:
        if not command:
            return b"-ERR empty command\r\n"

        cmd = command[0].upper()
        now = time.time()

        if cmd == b"PING":
            return b"+PONG\r\n"

        if cmd == b"ECHO" and len(command) == 2:
            return b"$%d\r\n%s\r\n" % (len(command[1]), command[1])

        if cmd == b"SET" and (len(command) == 3 or (len(command) == 5 and command[3].upper() == b"PX")):
            async with self._lock:
                self._store[command[1]] = command[2]
                if len(command) == 5:
                    try:
                        px = int(command[4])
                        self._ttl[command[1]] = now + px / 1000.0
                    except ValueError:
                        return b"-ERR PX value is not an integer\r\n"
            return b"+OK\r\n"

        if cmd == b"GET" and len(command) == 2:
            async with self._lock:
                if command[1] in self._ttl and self._ttl[command[1]] < now:
                    del self._store[command[1]]
                    del self._ttl[command[1]]
                value = self._store.get(command[1])
            if value is None:
                return b"$-1\r\n"
            return b"$%d\r\n%s\r\n" % (len(value), value)

        if cmd == b"DEL" and len(command) >= 2:
            deleted = 0
            async with self._lock:
                for key in command[1:]:
                    if key in self._store:
                        del self._store[key]
                        self._ttl.pop(key, None)
                        deleted += 1
            return b":%d\r\n" % deleted

        if cmd == b"EXISTS" and len(command) >= 2:
            count = 0
            async with self._lock:
                for key in command[1:]:
                    if key in self._ttl and self._ttl[key] < now:
                        del self._store[key]
                        del self._ttl[key]
                    if key in self._store:
                        count += 1
            return b":%d\r\n" % count

        if cmd == b"TTL" and len(command) == 2:
            async with self._lock:
                if command[1] not in self._store:
                    return b":-2\r\n"
                if command[1] not in self._ttl:
                    return b":-1\r\n"
                ttl_ms = int((self._ttl[command[1]] - now) * 1000)
                if ttl_ms < 0:
                    del self._store[command[1]]
                    del self._ttl[command[1]]
                    return b":-2\r\n"
            return b":%d\r\n" % ttl_ms

        if cmd == b"EXPIRE" and len(command) == 3:
            try:
                seconds = int(command[2])
            except ValueError:
                return b"-ERR invalid expire time\r\n"
            async with self._lock:
                if command[1] not in self._store:
                    return b":0\r\n"
                self._ttl[command[1]] = now + seconds
            return b":1\r\n"

        return b"-ERR unknown command\r\n"
