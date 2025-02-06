import logging
from collections import defaultdict
from typing import Dict, Literal, Optional, Set

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import TaskResultStatus, TaskStatus, TaskCancelConfirmStatus
from scaler.protocol.python.message import StateTask, Task, TaskCancel, TaskResult, TaskCancelConfirm
from scaler.protocol.python.status import TaskManagerStatus
from scaler.scheduler.graph_manager import VanillaGraphTaskManager
from scaler.scheduler.mixins import ClientManager, ObjectManager, TaskManager, WorkerManager
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.queues.async_indexed_queue import AsyncIndexedQueue
from scaler.utility.task.task_state_machine import TaskState


class VanillaTaskManager(TaskManager, Looper, Reporter):
    def __init__(self, max_number_of_tasks_waiting: int):
        self._max_number_of_tasks_waiting = max_number_of_tasks_waiting
        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None

        self._client_manager: Optional[ClientManager] = None
        self._object_manager: Optional[ObjectManager] = None
        self._worker_manager: Optional[WorkerManager] = None
        self._graph_manager: Optional[VanillaGraphTaskManager] = None

        self._task_id_to_task: Dict[bytes, Task] = dict()
        self._task_id_to_task_state: Dict[bytes, TaskState] = dict()

        self._unassigned: AsyncIndexedQueue = AsyncIndexedQueue()
        self._running: Set[bytes] = set()

        self._done_statistics = defaultdict(lambda: 0)

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncConnector,
        client_manager: ClientManager,
        object_manager: ObjectManager,
        worker_manager: WorkerManager,
        graph_manager: VanillaGraphTaskManager,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor

        self._client_manager = client_manager
        self._object_manager = object_manager
        self._worker_manager = worker_manager
        self._graph_manager = graph_manager

    async def routine(self):
        task_id = await self._unassigned.get()

        # FIXME: As the assign_task_to_worker() call can be blocking (especially if there is no worker connected to the
        # scheduler), we might end up with the task object being in neither _running nor _unassigned.
        # In this case, the scheduler will answer any task cancellation request with a "task not found" error, which is
        # a bug.
        # https://github.com/Citi/scaler/issues/45

        if not await self._worker_manager.on_assign_task(self._task_id_to_task[task_id]):
            await self._unassigned.put(task_id)
            return

        self._task_id_to_task_state[task_id].on_has_capacity()
        self._running.add(task_id)
        await self.__send_monitor(
            task_id, self._object_manager.get_object_name(self._task_id_to_task[task_id].func_object_id)
        )

    def get_status(self) -> TaskManagerStatus:
        return TaskManagerStatus.new_msg(
            unassigned=self._unassigned.qsize(),
            running=len(self._running),
            success=self._done_statistics["success"],
            failed=self._done_statistics["failed"],
            cancel_failed=self._done_statistics["cancel_failed"],
            canceled=self._done_statistics["canceled"],
            not_found=self._done_statistics["not_found"],
        )

    async def on_task_new(self, client: bytes, task: Task):
        if (
            0 <= self._max_number_of_tasks_waiting <= self._unassigned.qsize()
            and not self._worker_manager.has_available_worker()
        ):
            await self._binder.send(client, TaskResult.new_msg(task.task_id, TaskResultStatus.NoWorker))
            return

        self._client_manager.on_task_begin(client, task.task_id)
        self._task_id_to_task[task.task_id] = task
        self._task_id_to_task_state[task.task_id] = TaskState()

        await self._unassigned.put(task.task_id)
        await self.__send_monitor(
            task.task_id, self._object_manager.get_object_name(self._task_id_to_task[task.task_id].func_object_id)
        )

    async def on_task_cancel(self, client: bytes, task_cancel: TaskCancel):
        current_state = self.__adjust_task_state(task_cancel.task_id, "task_cancel")
        if current_state is None:
            logging.warning(f"cannot cancel, task not found: task_id={task_cancel.task_id.hex()}")
            return

        if task_cancel.task_id in self._unassigned:
            await self.on_task_result(TaskResult.new_msg(task_cancel.task_id, TaskStatus.Canceled))
            return

        await self._worker_manager.on_task_cancel(task_cancel)
        await self.__send_monitor(
            task_cancel.task_id,
            self._object_manager.get_object_name(self._task_id_to_task[task_cancel.task_id].func_object_id),
        )

    async def on_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        if task_cancel_confirm.task_id not in self._task_id_to_task_state:
            return

        if task_cancel_confirm.status == TaskCancelConfirmStatus.Canceled:
            current_state = self.__adjust_task_state(task_cancel_confirm.task_id, "task_cancel_confirm")
            self.__count_done_task_status("canceled")
        elif task_cancel_confirm.status == TaskCancelConfirmStatus.NotFound:
            current_state = self.__adjust_task_state(task_cancel_confirm.task_id, "task_cancel_confirm_not_found")
            self.__count_done_task_status("not_found")
        else:
            assert task_cancel_confirm.status == TaskCancelConfirmStatus.CancelFailed
            current_state = self.__adjust_task_state(task_cancel_confirm.task_id, "task_cancel_confirm_failed")
            self.__count_done_task_status("cancel_failed")

        if not self._task_id_to_task_state[task_cancel_confirm.task_id].canceled():
            return

        # task is canceled
        if task_cancel_confirm.task_id in self._unassigned:
            self._unassigned.remove(task_cancel_confirm.task_id)
        else:
            assert task_cancel_confirm.task_id in self._running
            self._running.remove(task_cancel_confirm.task_id)

        await self._worker_manager.on_task_canceled(task_cancel_confirm)

        await self.on_task_new(client, task_cancel_confirm.task)

    async def on_task_result(self, result: TaskResult):
        if result.task_id not in self._task_id_to_task_state:
            return

        self._task_id_to_task_state[result.task_id].on_result()
        if not self._task_id_to_task_state[result.task_id].finished():
            return

        # task is finished
        if result.status == TaskResultStatus.Success:
            self.__count_done_task_status("success")
        elif result.status == TaskResultStatus.Failed:
            self.__count_done_task_status("failed")
        elif result.status == TaskResultStatus.NoWorker:
            self.__count_done_task_status("no_worker")
        elif result.status == TaskResultStatus.WorkerDied:
            self.__count_done_task_status("worker_died")

        if result.task_id in self._unassigned:
            self._unassigned.remove(result.task_id)
        else:
            assert result.task_id in self._running
            self._running.remove(result.task_id)

        await self._worker_manager.on_task_finished(result)

        if result.task_id in self._task_id_to_task:
            func_object_name = self._object_manager.get_object_name(
                self._task_id_to_task.pop(result.task_id).func_object_id
            )
            client = self._client_manager.on_task_finish(result.task_id)
        else:
            func_object_name = b"<unknown func object>"
            client = None

        await self.__send_monitor(result.task_id, func_object_name, result.metadata)

        if self._graph_manager.is_graph_sub_task(result.task_id):
            await self._graph_manager.on_graph_sub_task_result(result)
            return

        if client is not None:
            await self._binder.send(client, result)

    async def on_no_cancel_reroute(self, task_id: bytes):
        assert self._client_manager.get_client_id(task_id) is not None

        self._running.remove(task_id)
        await self._unassigned.put(task_id)
        await self.__send_monitor(
            task_id, self._object_manager.get_object_name(self._task_id_to_task[task_id].func_object_id)
        )

    async def __send_monitor(self, task_id: bytes, function_name: bytes, metadata: Optional[bytes] = b""):
        worker = self._worker_manager.get_worker_by_task_id(task_id)

        task_state = (
            self._task_id_to_task_state.pop(task_id)
            if self._task_id_to_task_state[task_id].done()
            else self._task_id_to_task_state[task_id]
        )

        await self._binder_monitor.send(
            StateTask.new_msg(task_id, function_name, task_state.state().value, worker, metadata)
        )

    def __adjust_task_state(
        self,
        task_id: bytes,
        transition: Literal[
            "task",
            "has_capacity",
            "no_capacity",
            "task_result",
            "task_cancel",
            "task_cancel_confirm",
            "task_cancel_confirm_failed",
            "task_cancel_confirm_not_found",
            "task_cancel_confirm_with_task",
        ],
    ) -> Optional[TaskState]:
        """if adjust task state machine is successful, then return TaskState object associate with the task_id,
        return None otherwise"""
        if transition == "task":
            task_state = TaskState()
            self._task_id_to_task_state[task_id] = task_state
            return task_state

        task_state = self._task_id_to_task_state.get(task_id, None)
        if task_state is None:
            logging.error(f"unknown {transition=} for task_id={task_id.hex()}")
            return None

        success = False
        if transition == "has_capacity":
            success = task_state.on_has_capacity()
        elif transition == "no_capacity":
            success = task_state.on_no_capacity()
        elif transition == "task_result":
            success = task_state.on_result()
        elif transition == "task_cancel":
            success = task_state.on_cancel()
        elif transition == "task_cancel_confirm":
            success = task_state.on_cancel_confirm()
        elif transition == "task_cancel_confirm_failed":
            success = task_state.on_cancel_confirm_failed()
        elif transition == "task_cancel_confirm_not_found":
            success = task_state.on_cancel_confirm_not_found()
        elif transition == "task_cancel_confirm_with_task":
            success = task_state.on_cancel_confirm_with_task()

        return task_state if success else None

    def __count_done_task_status(
        self, status: Literal["success", "failed", "cancel_failed", "canceled", "not_found", "no_worker", "worker_died"]
    ):
        self._done_statistics[status] += 1
