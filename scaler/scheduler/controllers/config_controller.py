from argparse import Namespace
import dataclasses
import logging
from typing import Any, Dict, Type, TypeVar
import tomllib

from scaler.scheduler.config import SchedulerConfig
from scaler.scheduler.controllers.mixins import ConfigController


class VanillaConfigController(ConfigController):
    def __init__(self, config: SchedulerConfig):
        self._config = {}

        for key, value in config.__dict__.items():
            self.update_config(key, value)

    def get_config(self, path: str) -> Any:
        if path not in self._config:
            raise KeyError(f"No such config: `{path}`")

        return self._config[path]

    def update_config(self, path: str, value: Any):
        # TODO: please add update config message and let config able to handle update config on the fly

        if path not in self._config:
            self._config[path] = value
            logging.info(f"ConfigController: {path} = {value}")
            return

        old_value = self._config[path]
        self._config[path] = value
        logging.info(f"ConfigController: updated `{path}` from `{old_value}` to `{value}`")


T = TypeVar("T")


class TomlConfigController(ConfigController):
    """
    A controller that loads configuration from a TOML file, conforming to the
    ConfigController interface and providing enhanced dataclass loading.
    """

    def __init__(self, config_path: str):
        self._config_path = config_path
        self._config_data: Dict[str, Any] = {}
        try:
            with open(self._config_path, "rb") as f:
                self._config_data = tomllib.load(f)
                logging.info(f"Successfully loaded configuration from {self._config_path}")
        except FileNotFoundError:
            logging.error(f"Configuration file not found at: {self._config_path}")
            raise
        except tomllib.TOMLDecodeError as e:
            logging.error(f"Error decoding TOML file at {self._config_path}: {e}")
            raise

    def get_config(self, path: str) -> Any:
        """
        Gets a single configuration value by its key from the loaded TOML file.
        This method fulfills the ConfigController interface contract.

        Note: This is a simplified implementation. It does not support nested paths like 'section.key'.
        """
        # This implementation assumes a flat key-value structure for backward compatibility
        # It searches for the key across all sections.
        for section in self._config_data.values():
            if path in section:
                return section[path]
        raise KeyError(f"No such config: `{path}`")

    def get_dataclass(self, section_name: str, config_class: Type[T]) -> T:
        """
        Loads a whole section from the TOML file into a typed dataclass object.
        """
        if not dataclasses.is_dataclass(config_class):
            raise TypeError("config_class must be a dataclass.")

        section_data = self._config_data.get(section_name, {})
        return config_class(**section_data)

    def update_config(self, path: str, value: Any):
        """
        Creates or updates a configuration value in memory.
        Note: This does not save the change back to the .toml file.
        """
        # For simplicity, this updates the first section where the key is found or adds to a new 'runtime' section
        updated = False
        for section in self._config_data.values():
            if path in section:
                old_value = section[path]
                section[path] = value
                logging.info(f"ConfigController: updated `{path}` from `{old_value}` to `{value}`")
                updated = True
                break

        if not updated:
            if "runtime" not in self._config_data:
                self._config_data["runtime"] = {}
            self._config_data["runtime"][path] = value
            logging.info(f"ConfigController: {path} = {value}")

    def update_from_args(self, config_object: T, args: Namespace):
        """Overrides values in a config object with provided command-line arguments."""
        for key, value in vars(args).items():
            if not hasattr(config_object, key):
                setattr(config_object, key, value)
                logging.info(f"Config creation: updated `{key}` to `{value}`")
            elif value is not None and hasattr(config_object, key):
                old_value = getattr(config_object, key)
                setattr(config_object, key, value)
                logging.info(f"Config override: updated `{key}` from `{old_value}` to `{value}`")
