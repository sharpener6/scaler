import enum

from scaler.protocol.python.common import TaskStatus


class _Event(enum.Enum):
    HasCapacity = enum.auto()
    NoCapacity = enum.auto()
    TaskResult = enum.auto()
    TaskCancel = enum.auto()
    TaskCancelConfirm = enum.auto()
    TaskCancelConfirmFailed = enum.auto()
    TaskCancelConfirmNotFound = enum.auto()
    TaskCancelConfirmWitTask = enum.auto()


_TRANSITION_MAP: dict[TaskStatus, dict[_Event, TaskStatus]] = {
    TaskStatus.Inactive: {_Event.HasCapacity: TaskStatus.Running, _Event.NoCapacity: TaskStatus.Canceled},
    TaskStatus.Running: {_Event.TaskResult: TaskStatus.Finished, _Event.TaskCancel: TaskStatus.Canceling},
    TaskStatus.Canceling: {
        _Event.TaskCancel: TaskStatus.Canceling,
        _Event.TaskResult: TaskStatus.Canceling,
        _Event.TaskCancelConfirm: TaskStatus.Canceled,
        _Event.TaskCancelConfirmFailed: TaskStatus.Running,
        _Event.TaskCancelConfirmNotFound: TaskStatus.Canceled,
        _Event.TaskCancelConfirmWitTask: TaskStatus.Inactive,
    },
}


class TaskState:
    def __init__(self):
        self._state = TaskStatus.Inactive

    def __repr__(self):
        return self._state.value

    def state(self) -> TaskStatus:
        return self._state

    def finished(self):
        return self._state == TaskStatus.Finished

    def canceled(self):
        return self._state == TaskStatus.Canceled

    def done(self) -> bool:
        return self._state == TaskStatus.Finished or self._state == TaskStatus.Canceled

    def on_has_capacity(self) -> bool:
        return self.__on_event(_Event.HasCapacity)

    def on_no_capacity(self) -> bool:
        return self.__on_event(_Event.NoCapacity)

    def on_result(self) -> bool:
        return self.__on_event(_Event.TaskResult)

    def on_cancel(self) -> bool:
        return self.__on_event(_Event.TaskCancel)

    def on_cancel_confirm(self) -> bool:
        return self.__on_event(_Event.TaskCancelConfirm)

    def on_cancel_confirm_failed(self) -> bool:
        return self.__on_event(_Event.TaskCancelConfirmFailed)

    def on_cancel_confirm_not_found(self) -> bool:
        return self.__on_event(_Event.TaskCancelConfirmNotFound)

    def on_cancel_confirm_with_task(self) -> bool:
        return self.__on_event(_Event.TaskCancelConfirmWitTask)

    def __on_event(self, event: _Event) -> bool:
        options = _TRANSITION_MAP[self._state]

        if event not in options:
            return False

        self._state = options[event]
        return True
