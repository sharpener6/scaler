import dataclasses
from typing import Optional, Tuple

from scaler.utility.object_storage_config import ObjectStorageConfig


@dataclasses.dataclass
class ObjectStorageServerConfig:
    address: ObjectStorageConfig
    logging_paths: Tuple[str, ...] = ("/dev/stdout",)
    logging_level: str = "INFO"
    logging_config_file: Optional[str] = None
