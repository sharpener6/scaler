import dataclasses
from typing import Optional, Tuple

from scaler.io.config import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_WORKER_DEATH_TIMEOUT,
    DEFAULT_HARD_PROCESS_SUSPEND,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_NUMBER_OF_WORKER,
    DEFAULT_IO_THREADS,
)
from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.zmq_config import ZMQConfig


@dataclasses.dataclass
class ClusterConfig:
    scheduler_address: ZMQConfig
    object_storage_address: ObjectStorageConfig
    monitor_address: Optional[ZMQConfig] = None
    tags: str = ""
    worker_name_prefix: str = "worker"
    num_of_workers: int = DEFAULT_NUMBER_OF_WORKER
    io_threads: int = DEFAULT_IO_THREADS
    task_queue_size: int = DEFAULT_PER_WORKER_QUEUE_SIZE
    heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    garbage_collect_interval_seconds: int = DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS
    trim_memory_threshold_bytes: int = DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES
    task_timeout_seconds: int = DEFAULT_TASK_TIMEOUT_SECONDS
    death_timeout_seconds: int = DEFAULT_WORKER_DEATH_TIMEOUT
    hard_processor_suspend: bool = DEFAULT_HARD_PROCESS_SUSPEND
    event_loop: str = "builtin"
    logging_paths: Tuple[str, ...] = ("/dev/stdout",)
    logging_level: str = "INFO"
