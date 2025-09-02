import logging
from typing import Any

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
