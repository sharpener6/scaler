import dataclasses
import logging
import tomllib
import re
from abc import ABC
from argparse import Namespace
from dataclasses import InitVar
from typing import Type, TypeVar, Self, Tuple, List, Set, Optional, cast

from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.io import config as defaults
from scaler.utility.zmq_config import ZMQConfig, ZMQType


@dataclasses.dataclass
class Config(ABC):
    """Abstract base class for all configuration dataclasses."""

    def describe(self) -> str:
        """
        Returns a human-readable string representation of the current config.
        """
        lines = []
        for parent_field in dataclasses.fields(self):
            parent_name = parent_field.name
            nested_obj = getattr(self, parent_name)
            if dataclasses.is_dataclass(nested_obj):
                for child_field in dataclasses.fields(nested_obj):
                    key = f"{parent_name}.{child_field.name}"
                    value = getattr(nested_obj, child_field.name)
                    lines.append(f'{key} = "{value}"')
        return "\n".join(lines)

    def update_from_args(self, namespace: Namespace):
        """
        Updates the config values from command-line arguments.
        """
        args_dict = {k: v for k, v in vars(namespace).items() if v is not None}
        for field in dataclasses.fields(self):
            nested_obj = getattr(self, field.name)
            if dataclasses.is_dataclass(nested_obj):
                for nested_field in dataclasses.fields(nested_obj):
                    arg_key = nested_field.name
                    nested_arg_key = f"{field.name}_{nested_field.name}"

                    value_to_set = args_dict.get(nested_arg_key, args_dict.get(arg_key))

                    if value_to_set is not None:
                        old_value = getattr(nested_obj, nested_field.name)
                        setattr(nested_obj, nested_field.name, value_to_set)
                        logging.info(f"Config override: updated `{arg_key}` from `{old_value}` to `{value_to_set}`")

    @classmethod
    def from_toml(cls: Type[Self], filename: str) -> Self:
        """
        Loads the configuration from a TOML file.
        """
        try:
            with open(filename, "rb") as f:
                data = tomllib.load(f)
                logging.info(f"Successfully loaded configuration from {filename}")
        except FileNotFoundError:
            logging.warning(f"Configuration file not found at: {filename}. Using default values.")
            data = {}
        except tomllib.TOMLDecodeError as e:
            logging.error(f"Error decoding TOML file at {filename}: {e}")
            raise

        return cast(Self, _from_dict(cls, data))


ConfigType = TypeVar("ConfigType", bound=Config)


def _from_dict(cls: Type[ConfigType], data: dict) -> ConfigType:
    """
    Recursively instantiates a dataclass from a dictionary, handling nested
    dataclasses correctly.
    """
    field_types = {f.name: f.type for f in dataclasses.fields(cls)}
    kwargs = {}
    for key, value in data.items():
        if key in field_types:
            field_type = field_types[key]
            # Mypy needs a little help to know field_type is a type
            if isinstance(field_type, type) and dataclasses.is_dataclass(field_type) and isinstance(value, dict):
                kwargs[key] = _from_dict(field_type, value)
            else:
                kwargs[key] = value
    return cls(**kwargs)


@dataclasses.dataclass
class LoggingConfig:
    paths: Tuple[str, ...] = defaults.DEFAULT_LOGGING_PATHS
    level: str = defaults.DEFAULT_LOGGING_LEVEL
    config_file: str | None = None


@dataclasses.dataclass
class ObjectStorageConfig:
    host: str = "127.0.0.1"
    port: int = 6380
    address: ZMQConfig = dataclasses.field(init=False)

    def to_string(self) -> str:
        return f"tcp://{self.host}:{self.port}"

    @staticmethod
    def from_string(address: str) -> "ObjectStorageConfig":
        address_format = r"^tcp://([a-zA-Z0-9\.\-]+):([0-9]{1,5})$"
        match = re.compile(address_format).match(address)

        if not match:
            raise ValueError("object storage address has to be tcp://<host>:<port>")

        host = match.group(1)
        port = int(match.group(2))

        return ObjectStorageConfig(host=host, port=port)

    def __post_init__(self):
        self.address = ZMQConfig(type=ZMQType.tcp, host=self.host, port=self.port)


@dataclasses.dataclass
class SchedulerConfig:
    address_str: InitVar[str] = "tcp://127.0.0.1:6378"
    storage_address_str: InitVar[Optional[str]] = "tcp://127.0.0.1:6379"
    monitor_address_str: InitVar[Optional[str]] = "tcp://127.0.0.1:6380"
    address: ZMQConfig = dataclasses.field(init=False)
    storage_address: Optional[ObjectStorageConfig] = dataclasses.field(init=False)
    monitor_address: Optional[ZMQConfig] = dataclasses.field(init=False)
    protected: bool = True
    allocate_policy_str: InitVar[str] = "even"
    allocate_policy: AllocatePolicy = dataclasses.field(init=False)
    event_loop: str = "builtin"
    zmq_io_threads: int = defaults.DEFAULT_IO_THREADS
    max_number_of_tasks_waiting: int = defaults.DEFAULT_MAX_NUMBER_OF_TASKS_WAITING
    client_timeout_seconds: int = defaults.DEFAULT_CLIENT_TIMEOUT_SECONDS
    worker_timeout_seconds: int = defaults.DEFAULT_WORKER_TIMEOUT_SECONDS
    object_retention_seconds: int = defaults.DEFAULT_OBJECT_RETENTION_SECONDS
    load_balance_seconds: int = defaults.DEFAULT_LOAD_BALANCE_SECONDS
    load_balance_trigger_times: int = defaults.DEFAULT_LOAD_BALANCE_TRIGGER_TIMES

    def __post_init__(
        self,
        address_str: str,
        storage_address_str: Optional[str],
        monitor_address_str: Optional[str],
        allocate_policy_str: str,
    ):
        self.address = ZMQConfig.from_string(address_str)
        self.storage_address = ObjectStorageConfig.from_string(storage_address_str) if storage_address_str else None
        self.monitor_address = ZMQConfig.from_string(monitor_address_str) if monitor_address_str else None
        self.allocate_policy = AllocatePolicy[allocate_policy_str]


@dataclasses.dataclass
class WorkerConfig:
    name: str = "DefaultWorker"
    num_of_workers: int = defaults.DEFAULT_NUMBER_OF_WORKER
    io_threads: int = defaults.DEFAULT_IO_THREADS
    per_worker_task_queue_size: int = defaults.DEFAULT_PER_WORKER_QUEUE_SIZE
    heartbeat_interval_seconds: int = defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    garbage_collect_interval_seconds: int = defaults.DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS
    trim_memory_threshold_bytes: int = defaults.DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES
    task_timeout_seconds: int = defaults.DEFAULT_TASK_TIMEOUT_SECONDS
    death_timeout_seconds: int = defaults.DEFAULT_WORKER_DEATH_TIMEOUT
    hard_processor_suspend: bool = defaults.DEFAULT_HARD_PROCESSOR_SUSPEND


@dataclasses.dataclass
class ClusterConfig:
    worker_names_str: InitVar[str] = "worker"
    worker_tags_str: InitVar[str] = ""
    num_of_workers: int = defaults.DEFAULT_NUMBER_OF_WORKER
    worker_names: List[str] = dataclasses.field(init=False)
    worker_tags: Set[str] = dataclasses.field(init=False)

    def __post_init__(self, worker_names_str: str, worker_tags_str: str):
        if worker_tags_str:
            self.worker_tags = set(tag.strip() for tag in worker_tags_str.split(","))
        else:
            self.worker_tags = set()

        if worker_names_str:
            self.worker_names = list(name.strip() for name in worker_names_str.split(","))
        else:
            self.worker_names = list()


@dataclasses.dataclass
class WebUIConfig:
    web_host: str = "127.0.0.1"
    web_port: int = 50001
    sched_address: str = "tcp://127.0.0.1:6379"


@dataclasses.dataclass
class ScalerConfig(Config):
    scheduler: SchedulerConfig = dataclasses.field(default_factory=SchedulerConfig)
    worker: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    webui: WebUIConfig = dataclasses.field(default_factory=WebUIConfig)
    object_storage: ObjectStorageConfig = dataclasses.field(default_factory=ObjectStorageConfig)
    cluster: ClusterConfig = dataclasses.field(default_factory=ClusterConfig)
    logging: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
