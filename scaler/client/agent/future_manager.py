import logging
import threading
from concurrent.futures import Future, InvalidStateError
from typing import Dict

from scaler.client.agent.mixins import FutureManager
from scaler.client.future import ScalerFuture
from scaler.client.serializer.mixins import Serializer
from scaler.protocol.python.common import TaskResultType, TaskCancelConfirmType, TaskState
from scaler.protocol.python.message import TaskResult, TaskCancelConfirm
from scaler.utility.exceptions import WorkerDiedError
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.utility.metadata.profile_result import retrieve_profiling_result_from_task_result


class ClientFutureManager(FutureManager):
    def __init__(self, serializer: Serializer):
        self._lock = threading.RLock()
        self._serializer = serializer

        self._task_id_to_future: Dict[TaskID, ScalerFuture] = dict()

    def add_future(self, future: Future):
        assert isinstance(future, ScalerFuture)
        with self._lock:
            future.set_running_or_notify_cancel()
            self._task_id_to_future[future.task_id] = future

    def cancel_all_futures(self):
        with self._lock:
            logging.info(f"canceling {len(self._task_id_to_future)} task(s)")
            for task_id, future in self._task_id_to_future.items():
                future.cancel()

    def set_all_futures_with_exception(self, exception: Exception):
        with self._lock:
            for future in self._task_id_to_future.values():
                try:
                    future.set_exception(exception)
                except InvalidStateError:
                    continue  # Future got canceled

            self._task_id_to_future.clear()

    def on_task_result(self, result: TaskResult):
        with self._lock:
            task_id = result.task_id
            if task_id not in self._task_id_to_future:
                return

            future = self._task_id_to_future.pop(task_id)
            assert result.task_id == future.task_id

            profile_result = retrieve_profiling_result_from_task_result(result)

            match result.result_type:
                case TaskResultType.FailedWorkerDied:
                    future.set_exception(
                        WorkerDiedError(f"worker died when processing task: {task_id.hex()}"), profile_result
                    )

                case TaskResultType.Success:
                    assert len(result.results) == 1
                    future.set_result_ready(ObjectID(result.results[0]), TaskState.Success, profile_result)

                case TaskResultType.Failed:
                    assert len(result.results) == 1
                    future.set_result_ready(ObjectID(result.results[0]), TaskState.Failed, profile_result)

                case _:
                    raise TypeError(f"{result.task_id.hex()}: Unknown task status: {result.result_type}")

    def on_task_cancel_confirm(self, cancel_confirm: TaskCancelConfirm):
        with self._lock:
            task_id = cancel_confirm.task_id
            if task_id not in self._task_id_to_future:
                return

            future = self._task_id_to_future.pop(task_id)
            assert cancel_confirm.task_id == future.task_id

            match cancel_confirm.cancel_confirm_type:
                case TaskCancelConfirmType.Canceled:
                    future.set_canceled()

                case TaskCancelConfirmType.CancelNotFound:
                    logging.error(f"{task_id!r}: task not found")
                    future.set_canceled()

                case TaskCancelConfirmType.CancelFailed:
                    logging.error(f"{task_id!r}: task cancel failed")
                    self._task_id_to_future[task_id] = future

                case _:
                    raise TypeError(
                        f"{task_id}: unknown task cancel confirm type:" f" {cancel_confirm.cancel_confirm_type}"
                    )
