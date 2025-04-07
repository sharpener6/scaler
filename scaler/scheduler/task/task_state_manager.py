import logging
from typing import Dict, Optional, Set, List

from scaler.protocol.python.common import TaskState, TaskTransition
from scaler.scheduler.task.task_state_machine import TaskStateMachine


class TaskStateManager:
    def __init__(self, debug: bool):
        self._debug = debug
        self._task_id_to_state_machine: Dict[bytes, TaskStateMachine] = dict()
        self._statistics: Dict[TaskState, int] = {state: 0 for state in TaskState}

    def on_transition(self, task_id: bytes, transition: TaskTransition) -> Optional[TaskStateMachine]:
        """if adjust task state machine is successful, then return TaskStateFlags object associate with the task_id,
        return None otherwise

        This should be a central place to synchronize task state machine, if any unexpected event happened, it will not
        return the TaskStateFlags
        """
        if transition == TaskTransition.Task:
            state_machine = TaskStateMachine(self._debug)
            self._task_id_to_state_machine[task_id] = state_machine
            self._statistics[state_machine.state()] += 1
            return state_machine

        task_state_machine = self._task_id_to_state_machine.get(task_id, None)
        if task_state_machine is None:
            logging.error(f"Task[{task_id.hex()}]: unknown {transition=} for current_state={task_state_machine}")
            return None

        transit_success = task_state_machine.on_transition(transition)
        if transit_success:
            self._statistics[task_state_machine.previous_state()] -= 1
            self._statistics[task_state_machine.state()] += 1
        else:
            logging.error(f"Task[{task_id.hex()}]: cannot apply {transition} to state {task_state_machine.state()}")

        return task_state_machine if transit_success else None

    def clear_task(self, task_id: bytes):
        self._task_id_to_state_machine.pop(task_id)

    def get_task_state_machine(self, task_id: bytes) -> TaskStateMachine:
        return self._task_id_to_state_machine[task_id]

    def get_statistics(self) -> Dict[TaskState, int]:
        return self._statistics

    def get_paths(self, task_ids: Set[bytes]) -> Dict[str, List[str]]:
        if not task_ids:
            return {
                task_id.hex(): state_machine.get_path()
                for task_id, state_machine in self._task_id_to_state_machine.items()
            }

        return {
            task_id.hex(): state_machine.get_path()
            for task_id, state_machine in self._task_id_to_state_machine.items()
            if task_id in task_ids
        }
