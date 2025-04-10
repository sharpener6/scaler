import dataclasses

from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.zmq_config import ZMQConfig


@dataclasses.dataclass
class SchedulerConfig:
    event_loop: str
    address: ZMQConfig
    object_storage_config: ObjectStorageConfig
    io_threads: int
    max_number_of_tasks_waiting: int
    per_worker_queue_size: int
    client_timeout_seconds: int
    worker_timeout_seconds: int
    object_retention_seconds: int
    load_balance_seconds: int
    load_balance_trigger_times: int
    protected: bool
