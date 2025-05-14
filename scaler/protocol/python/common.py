import dataclasses
import enum
from typing import List, Tuple

from scaler.protocol.capnp._python import _common  # noqa
from scaler.protocol.python.mixins import Message


class TaskResultType(enum.Enum):
    Success = _common.TaskResultType.success  # if submit and task is done and get result
    Failed = _common.TaskResultType.failed  # if submit and task is failed on worker
    FailedWorkerDied = _common.TaskResultType.failedWorkerDied  # if submit and worker died


class TaskCancelConfirmType(enum.Enum):
    Canceled = _common.TaskCancelConfirmType.canceled  # if cancel success
    CancelFailed = _common.TaskCancelConfirmType.cancelFailed  # if failed to cancel if task is running
    CancelNotFound = _common.TaskCancelConfirmType.cancelNotFound  # if try to cancel, and task is not found


class TaskTransition(enum.Enum):
    Task = _common.TaskTransition.task
    HasCapacity = _common.TaskTransition.hasCapacity
    TaskResultSuccess = _common.TaskTransition.taskResultSuccess
    TaskResultFailed = _common.TaskTransition.taskResultFailed
    TaskResultWorkerDied = _common.TaskTransition.taskResultWorkerDied
    TaskCancel = _common.TaskTransition.taskCancel
    TaskCancelConfirmCanceled = _common.TaskTransition.taskCancelConfirmCanceled
    TaskCancelConfirmFailed = _common.TaskTransition.taskCancelConfirmFailed
    TaskCancelConfirmNotFound = _common.TaskTransition.taskCancelConfirmNotFound
    BalanceTaskCancel = _common.TaskTransition.balanceTaskCancel
    WorkerDisconnect = _common.TaskTransition.workerDisconnect
    SchedulerHasTask = _common.TaskTransition.schedulerHasTask
    SchedulerHasNoTask = _common.TaskTransition.schedulerHasNoTask


class TaskState(enum.Enum):
    Inactive = _common.TaskState.inactive  # task is scheduled but not allocate to worker
    Running = _common.TaskState.running  # task is running in worker
    Canceling = _common.TaskState.canceling  # task is canceling state
    BalanceCanceling = _common.TaskState.balanceCanceling  # task is in balance canceling state
    Success = _common.TaskState.success  # task is finished properly
    Failed = _common.TaskState.failed  # task is finished but exception happened
    FailedWorkerDied = _common.TaskState.failedWorkerDied  # task is failed due to worker died
    Canceled = _common.TaskState.canceled  # task is canceled (received task cancel confirm)
    CanceledNotFound = _common.TaskState.canceledNotFound  # task is not found when trying to cancel
    WorkerDisconnecting = _common.TaskState.workerDisconnecting  # task is lost due to worker disconnecting


@dataclasses.dataclass
class ObjectContent(Message):
    class ObjectContentType(enum.Enum):
        # FIXME: Pycapnp does not support assignment of raw enum values when the enum is itself declared within a list.
        # However, assigning the enum's string value works.
        # See https://github.com/capnproto/pycapnp/issues/374

        Serializer = "serializer"
        Object = "object"

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def object_ids(self) -> Tuple[bytes, ...]:
        return tuple(self._msg.objectIds)

    @property
    def object_types(self) -> Tuple[ObjectContentType, ...]:
        return tuple(ObjectContent.ObjectContentType(object_type._as_str()) for object_type in self._msg.objectTypes)

    @property
    def object_names(self) -> Tuple[bytes, ...]:
        return tuple(self._msg.objectNames)

    @property
    def object_bytes(self) -> Tuple[List[bytes], ...]:
        return tuple(self._msg.objectBytes)

    @staticmethod
    def new_msg(
        object_ids: Tuple[bytes, ...],
        object_types: Tuple[ObjectContentType, ...] = tuple(),
        object_names: Tuple[bytes, ...] = tuple(),
        object_bytes: Tuple[List[bytes], ...] = tuple(),
    ) -> "ObjectContent":
        return ObjectContent(
            _common.ObjectContent(
                objectIds=list(object_ids),
                objectTypes=[object_type.value for object_type in object_types],
                objectNames=list(object_names),
                objectBytes=tuple(object_bytes),
            )
        )

    def get_message(self):
        return self._msg
