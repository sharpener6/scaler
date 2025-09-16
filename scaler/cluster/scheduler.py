import asyncio
import multiprocessing
import signal
from asyncio import AbstractEventLoop, Task
from typing import Any, Optional, Tuple

from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.config import SchedulerConfig
from scaler.scheduler.scheduler import Scheduler, scheduler_main
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.config import ObjectStorageConfig
from scaler.utility.zmq_config import ZMQConfig


class SchedulerProcess(multiprocessing.get_context("spawn").Process):  # type: ignore[misc]
    def __init__(
        self,
        address: ZMQConfig,
        storage_address: Optional[ObjectStorageConfig],
        monitor_address: Optional[ZMQConfig],
        io_threads: int,
        max_number_of_tasks_waiting: int,
        client_timeout_seconds: int,
        worker_timeout_seconds: int,
        object_retention_seconds: int,
        load_balance_seconds: int,
        load_balance_trigger_times: int,
        protected: bool,
        allocate_policy: AllocatePolicy,
        event_loop: str,
        logging_paths: Tuple[str, ...],
        logging_config_file: Optional[str],
        logging_level: str,
    ):
        multiprocessing.Process.__init__(self, name="Scheduler")
        self._scheduler_config = SchedulerConfig(
            event_loop=event_loop,
            address_str=ZMQConfig.to_address(address),
            storage_address_str=storage_address.to_string() if storage_address is not None else None,
            monitor_address_str=monitor_address.to_address() if monitor_address is not None else None,
            zmq_io_threads=io_threads,
            max_number_of_tasks_waiting=max_number_of_tasks_waiting,
            client_timeout_seconds=client_timeout_seconds,
            worker_timeout_seconds=worker_timeout_seconds,
            object_retention_seconds=object_retention_seconds,
            load_balance_seconds=load_balance_seconds,
            load_balance_trigger_times=load_balance_trigger_times,
            protected=protected,
            allocate_policy_str=allocate_policy.name,
        )

        self._logging_paths = logging_paths
        self._logging_config_file = logging_config_file
        self._logging_level = logging_level

        self._scheduler: Optional[Scheduler] = None
        self._loop: Optional[AbstractEventLoop] = None
        self._task: Optional[Task[Any]] = None

    def run(self) -> None:
        # scheduler have its own single process
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
        register_event_loop(self._scheduler_config.event_loop)

        self._loop = asyncio.get_event_loop()
        SchedulerProcess.__register_signal(self._loop)

        self._task = self._loop.create_task(scheduler_main(self._scheduler_config))

        self._loop.run_until_complete(self._task)

    @staticmethod
    def __register_signal(loop):
        loop.add_signal_handler(signal.SIGINT, SchedulerProcess.__handle_signal)
        loop.add_signal_handler(signal.SIGTERM, SchedulerProcess.__handle_signal)

    @staticmethod
    def __handle_signal():
        for task in asyncio.all_tasks():
            task.cancel()
