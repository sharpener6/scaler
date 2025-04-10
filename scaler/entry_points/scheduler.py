import argparse

from scaler.io.config import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.object_storage.mini_redis_server_process import MiniRedisProcess
from scaler.scheduler.scheduler_process import SchedulerProcess
from scaler.utility.event_loop import EventLoopType
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_utility import get_available_tcp_port
from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.zmq_config import ZMQConfig


def get_args():
    parser = argparse.ArgumentParser("scaler scheduler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--io-threads", type=int, default=DEFAULT_IO_THREADS, help="number of io threads for zmq")
    parser.add_argument(
        "--object-storage-address",
        "-osa",
        type=str,
        default=None,
        help="specify object storage address, if not specify, scaler will spawn a simple object storage server that "
        "follow redis RESP protocol, also it only supports limit functionalities, the object storage address will be"
        "send to client and workers so they can talk to object storage server directly, e.g.: localhost:6379",
    )
    parser.add_argument(
        "--max-number-of-tasks-waiting",
        "-mt",
        type=int,
        default=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
        help="max number of tasks can wait in scheduler while all workers are full",
    )
    parser.add_argument(
        "--client-timeout-seconds",
        "-ct",
        type=int,
        default=DEFAULT_CLIENT_TIMEOUT_SECONDS,
        help="discard client when timeout seconds reached",
    )
    parser.add_argument(
        "--worker-timeout-seconds",
        "-wt",
        type=int,
        default=DEFAULT_WORKER_TIMEOUT_SECONDS,
        help="discard worker when timeout seconds reached",
    )
    parser.add_argument(
        "--object-retention-seconds",
        "-ot",
        type=int,
        default=DEFAULT_OBJECT_RETENTION_SECONDS,
        help="discard function in scheduler when timeout seconds reached",
    )
    parser.add_argument(
        "--load-balance-seconds",
        "-ls",
        type=int,
        default=DEFAULT_LOAD_BALANCE_SECONDS,
        help="number of seconds for load balance operation in scheduler",
    )
    parser.add_argument(
        "--load-balance-trigger-times",
        "-lbt",
        type=int,
        default=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
        help="exact number of repeated load balance advices when trigger load balance operation in scheduler",
    )
    parser.add_argument(
        "--per-worker-queue-size",
        "-qs",
        type=int,
        default=DEFAULT_PER_WORKER_QUEUE_SIZE,
        help="specify per worker queue size",
    )
    parser.add_argument(
        "--event-loop", "-e", default="builtin", choices=EventLoopType.allowed_types(), help="select event loop type"
    )
    parser.add_argument(
        "--protected", "-p", action="store_true", help="protect scheduler and worker from being shutdown by client"
    )
    parser.add_argument(
        "--logging-paths",
        "-lp",
        nargs="*",
        type=str,
        default=("/dev/stdout",),
        help="specify where scheduler log should logged to, it can accept multiple files, default is /dev/stdout",
    )
    parser.add_argument(
        "--logging-level",
        "-ll",
        type=str,
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
        help="specify the logging level",
    )
    parser.add_argument(
        "--logging-config-file",
        "-lc",
        type=str,
        default=None,
        help="use standard python the .conf file the specify python logging file configuration format, this will "
        "bypass --logging-path",
    )
    parser.add_argument("address", type=ZMQConfig.from_string, help="scheduler address to connect to")
    return parser.parse_args()


def main():
    args = get_args()
    setup_logger(args.logging_paths, args.logging_config_file, args.logging_level)

    if not args.object_storage_address:
        object_storage_address = ObjectStorageConfig.from_string(f"localhost:{get_available_tcp_port()}")
        object_storage = MiniRedisProcess(
            host=object_storage_address.host,
            port=object_storage_address.port,
            idle_timeout=5,
            command_timeout=5,
            logging_paths=args.logging_paths,
            logging_config_file=args.logging_config_file,
        )
        object_storage.start()
    else:
        object_storage_address = ObjectStorageConfig.from_string(args.object_storage_address)
        object_storage = None

    scheduler = SchedulerProcess(
        event_loop=args.event_loop,
        address=args.address,
        io_threads=args.io_threads,
        object_storage_config=object_storage_address,
        max_number_of_tasks_waiting=args.max_number_of_tasks_waiting,
        per_worker_queue_size=args.per_worker_queue_size,
        client_timeout_seconds=args.client_timeout_seconds,
        worker_timeout_seconds=args.worker_timeout_seconds,
        object_retention_seconds=args.object_retention_seconds,
        load_balance_seconds=args.load_balance_seconds,
        load_balance_trigger_times=args.load_balance_trigger_times,
        protected=args.protected,
        logging_paths=args.logging_paths,
        logging_config_file=args.logging_config_file,
    )
    scheduler.start()

    try:
        if object_storage is not None:
            object_storage.join()
        scheduler.join()
    except KeyboardInterrupt as e:
        pass

    finally:
        if object_storage is not None:
            object_storage.terminate()
        scheduler.terminate()
