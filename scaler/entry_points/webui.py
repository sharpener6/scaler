import argparse

from scaler.scheduler.controllers.config_controller import TomlConfigController
from scaler.ui.config import WebUIConfig
from scaler.ui.webui import start_webui


def get_args():
    parser = argparse.ArgumentParser(
        "web ui for scaler monitoring", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", type=str, default="webui.toml", help="Path to the TOML configuration file.")
    parser.add_argument("address", type=str, help="scheduler ipc address to connect to")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="host for webserver to connect to")
    parser.add_argument("--port", type=int, default=50001, help="port for webserver to connect to")
    return parser.parse_args()


def main():
    args = get_args()

    config_controller = TomlConfigController(args.config)
    webui_config = config_controller.get_dataclass("webui", WebUIConfig)
    config_controller.update_from_args(webui_config, args)

    start_webui(webui_config.address, webui_config.host, webui_config.port)
