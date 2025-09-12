import argparse
import logging

from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.scheduler.controllers.config_controller import TomlConfigController
from scaler.utility.logging.utility import setup_logger, get_logger_info
from scaler.utility.object_storage_config import ObjectStorageConfig


def get_args():
    parser = argparse.ArgumentParser(
        "scaler object storage server", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--config", type=str, default="object_storage_server.toml", help="Path to the TOML configuration file."
    )
    parser.add_argument(
        "address",
        type=ObjectStorageConfig.from_string,
        help="specify the object storage server address to listen to, e.g. tcp://localhost:2345.",
    )
    return parser.parse_args()


def main():
    args = get_args()

    config_controller = TomlConfigController(args.config)
    oss_config = config_controller.get_dataclass("object_storage_server", ObjectStorageConfig)
    config_controller.update_from_args(oss_config, args)

    setup_logger("object_storage_server")

    log_format_str, log_level_str, log_paths = get_logger_info(logging.getLogger())

    ObjectStorageServer().run(
        config_controller.address.host, args.address.port, log_level_str, log_format_str, log_paths
    )
