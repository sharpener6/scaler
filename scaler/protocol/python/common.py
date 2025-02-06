import dataclasses
import enum
from typing import List, Tuple

from scaler.protocol.capnp._python import _common  # noqa
from scaler.protocol.python.mixins import Message


class TaskResultStatus(enum.Enum):
    # task is accepted by scheduler, but will have below status
    Success = _common.TaskResultStatus.success  # if submit and task is done and get result
    Failed = _common.TaskResultStatus.failed  # if submit and task is failed on worker
    WorkerDied = (
        _common.TaskResultStatus.workerDied
    )  # if submit and worker died (only happened when scheduler keep_task=False)
    NoWorker = _common.TaskResultStatus.noWorker  # if submit and scheduler is full (not implemented yet)


class TaskCancelConfirmStatus(enum.Enum):
    Canceled = _common.TaskCancelConfirmStatus.canceled  # if cancel success
    CancelFailed = _common.TaskCancelConfirmStatus.cancelFailed  # if failed to cancel
    NotFound = _common.TaskCancelConfirmStatus.notFound  # if try to cancel, and task is not found


class TaskStatus(enum):
    # below are only used for monitoring channel, not sent to client
    Inactive = _common.TaskStatus.inactive  # task is scheduled but not allocate to worker
    Running = _common.TaskStatus.running  # task is running in worker
    Finished = _common.TaskStatus.finished  # task is finished (received task_result)
    Canceling = _common.TaskStatus.canceling  # task is canceling (can be in Inactive or Running state)
    Canceled = _common.TaskStatus.canceled  # task is canceled (received task cancel confirm)


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
