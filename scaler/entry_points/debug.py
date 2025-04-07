import argparse
import json

import zmq

from scaler.io.sync_connector import SyncConnector
from scaler.protocol.python.message import DebugCommand, DebugCommandResponse
from scaler.utility.zmq_config import ZMQConfig


def get_args():
    parser = argparse.ArgumentParser("debug client", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--host", "-H", default="tcp://127.0.0.1:2345", help="specify scaler scheduler host:port to get info"
    )
    parser.add_argument("command", nargs="+")
    return parser.parse_args()


def main():
    args = get_args()
    connector = SyncConnector(
        zmq.Context(), socket_type=zmq.DEALER, address=ZMQConfig.from_string(args.host), identity=b"debug_client"
    )

    command_bytes = " ".join(args.command).encode()
    connector.send(DebugCommand.new_msg(command=command_bytes))
    message = connector.receive()
    if not isinstance(message, DebugCommandResponse):
        raise TypeError(f"Received incorrect response: {message}")

    print(json.loads(message.response))
