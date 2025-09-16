import logging
import multiprocessing
from typing import Optional, Tuple

from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.utility.logging.utility import setup_logger, get_logger_info
from scaler.config import ObjectStorageConfig


class ObjectStorageServerProcess(multiprocessing.get_context("fork").Process):  # type: ignore[misc]
    def __init__(
        self,
        storage_address: ObjectStorageConfig,
        logging_paths: Tuple[str, ...],
        logging_level: str,
        logging_config_file: Optional[str],
    ):
        multiprocessing.Process.__init__(self, name="ObjectStorageServer")

        self._logging_paths = logging_paths
        self._logging_level = logging_level
        self._logging_config_file = logging_config_file

        self._storage_address = storage_address

        self._server = ObjectStorageServer()

    def wait_until_ready(self) -> None:
        """Blocks until the object storage server is available to server requests."""
        self._server.wait_until_ready()

    def run(self) -> None:
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
        logging.info(f"ObjectStorageServer: start and listen to {self._storage_address.to_string()}")

        log_format_str, log_level_str, logging_paths = get_logger_info(logging.getLogger())

        self._server.run(
            self._storage_address.host, self._storage_address.port, log_level_str, log_format_str, logging_paths
        )
