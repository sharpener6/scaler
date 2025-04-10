import asyncio
import functools
import multiprocessing
import signal
from asyncio import AbstractEventLoop, Task
from typing import Optional, Tuple, Any

from scaler.object_storage.mini_redis_server import MiniRedisServer
from scaler.utility.logging.utility import setup_logger


class MiniRedisProcess(multiprocessing.Process):
    def __init__(
        self,
        host: str,
        port: int,
        idle_timeout: int,
        command_timeout: int,
        logging_paths: Tuple[str, ...],
        logging_config_file: Optional[str],
    ):
        super().__init__()
        self._host = host
        self._port = port
        self._idle_timeout = idle_timeout
        self._command_timeout = command_timeout
        self._logging_paths = logging_paths
        self._logging_config_file = logging_config_file

        self._server: Optional[MiniRedisServer] = None

        self._loop: Optional[AbstractEventLoop] = None
        self._task: Optional[Task[Any]] = None

    def run(self) -> None:
        setup_logger(self._logging_paths, self._logging_config_file)

        self._loop = asyncio.get_event_loop()
        _register_signal(self._loop)

        self._server = MiniRedisServer(
            host=self._host, port=self._port, idle_timeout=self._idle_timeout, command_timeout=self._command_timeout
        )

        self._task = self._loop.create_task(self._server.start())

        self._loop.run_until_complete(self._task)


def _register_signal(loop):
    loop.add_signal_handler(signal.SIGINT, functools.partial(_handle_signal))
    loop.add_signal_handler(signal.SIGTERM, functools.partial(_handle_signal))


def _handle_signal():
    for task in asyncio.all_tasks():
        task.cancel()
