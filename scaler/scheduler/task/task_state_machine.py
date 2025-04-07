import enum
from typing import Optional, List

from scaler.protocol.python.common import TaskTransition, TaskState


class TaskFlags(enum.Flag):
    GraphManager = enum.auto()


class TaskStateMachine:
    TRANSITION_MAP: dict[TaskState, dict[TaskTransition, TaskState]] = {
        TaskState.Inactive: {
            TaskTransition.HasCapacity: TaskState.Running,
            TaskTransition.TaskCancel: TaskState.Canceled,
        },
        TaskState.Canceling: {
            TaskTransition.TaskCancelConfirmCanceled: TaskState.Canceled,
            TaskTransition.WorkerDisconnect: TaskState.Canceled,
            TaskTransition.TaskCancelConfirmFailed: TaskState.Running,
            TaskTransition.TaskCancelConfirmNotFound: TaskState.CanceledNotFound,
        },
        TaskState.Running: {
            TaskTransition.TaskResultSuccess: TaskState.Success,
            TaskTransition.TaskResultFailed: TaskState.Failed,
            TaskTransition.TaskResultWorkerDied: TaskState.FailedWorkerDied,
            TaskTransition.TaskCancel: TaskState.Canceling,
            TaskTransition.BalanceTaskCancel: TaskState.BalanceCanceling,
            TaskTransition.WorkerDisconnect: TaskState.WorkerDisconnecting,
        },
        TaskState.BalanceCanceling: {
            TaskTransition.TaskResultSuccess: TaskState.Success,
            TaskTransition.TaskResultFailed: TaskState.Failed,
            TaskTransition.TaskResultWorkerDied: TaskState.FailedWorkerDied,
            TaskTransition.TaskCancel: TaskState.Canceling,
            TaskTransition.TaskCancelConfirmCanceled: TaskState.Inactive,
            TaskTransition.TaskCancelConfirmFailed: TaskState.Running,
            TaskTransition.WorkerDisconnect: TaskState.WorkerDisconnecting,
        },
        TaskState.WorkerDisconnecting: {
            TaskTransition.SchedulerHasTask: TaskState.Inactive,
            TaskTransition.SchedulerHasNoTask: TaskState.FailedWorkerDied,
        },
    }

    def __init__(self, debug):
        self._debug = debug
        self._paths = list()

        self._previous_state = None
        self._state = TaskState.Inactive

        self._flags = TaskFlags(0)

    def __repr__(self):
        return f"TaskStateMachine(previous_state={self._previous_state}, state={self._state}, flags={self._flags})"

    def get_path(self) -> List[str]:
        result = []
        for state, transition in self._paths:
            result.append(state.name)
            result.append(transition.name)

        result.append(self._state.name)
        return result

    def previous_state(self) -> Optional[TaskState]:
        return self._previous_state

    def state(self) -> TaskState:
        return self._state

    def is_running(self) -> bool:
        return self._state == TaskState.Running

    def is_canceling(self) -> bool:
        return self._state == TaskState.Canceling

    def is_finished(self) -> bool:
        return self._state in {TaskState.Success, TaskState.Failed, TaskState.FailedWorkerDied}

    def is_canceled(self) -> bool:
        return self._state in {TaskState.Canceled, TaskState.CanceledNotFound}

    def is_done(self) -> bool:
        return self.is_finished() or self.is_canceled()

    def on_transition(self, transition: TaskTransition) -> bool:
        if self._state not in TaskStateMachine.TRANSITION_MAP:
            return False

        options = TaskStateMachine.TRANSITION_MAP[self._state]
        if transition not in options:
            return False

        if self._debug:
            self._paths.append((self._state, transition))

        self._previous_state = self._state
        self._state = options[transition]
        return True

    def add_flag(self, flag: TaskFlags):
        self._flags |= flag

    def remove_flag(self, flag: TaskFlags):
        self._flags ^= flag
