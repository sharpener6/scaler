import logging
import time
from typing import Dict, List, Optional, Set, Tuple

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import TaskStatus
from scaler.io.config import DEFAULT_PER_WORKER_QUEUE_SIZE
from scaler.protocol.python.message import (
    ClientDisconnect,
    DisconnectRequest,
    DisconnectResponse,
    StateWorker,
    Task,
    TaskCancel,
    TaskResult,
    WorkerHeartbeat,
    WorkerHeartbeatEcho,
)
from scaler.protocol.python.status import ProcessorStatus, Resource, WorkerManagerStatus, WorkerStatus
from scaler.scheduler.allocate_policy.mixins import TaskAllocatePolicy
from scaler.scheduler.managers.mixins import TaskManager, WorkerManager
from scaler.utility.mixins import Looper, Reporter

UINT8_MAX = 2**8 - 1


class VanillaWorkerManager(WorkerManager, Looper, Reporter):
    def __init__(self, timeout_seconds: int, task_allocate_policy: TaskAllocatePolicy):
        self._timeout_seconds = timeout_seconds

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None
        self._task_manager: Optional[TaskManager] = None

        self._worker_alive_since: Dict[bytes, Tuple[float, WorkerHeartbeat]] = dict()
        self._allocator_policy = task_allocate_policy

    def register(self, binder: AsyncBinder, binder_monitor: AsyncConnector, task_manager: TaskManager):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._task_manager = task_manager

    async def assign_task_to_worker(self, task: Task) -> bool:
        worker = await self._allocator_policy.assign_task(task.task_id)
        if worker is None:
            return False

        # send to worker
        await self._binder.send(worker, task)
        return True

    async def on_task_cancel(self, task_cancel: TaskCancel):
        worker = self._allocator_policy.remove_task(task_cancel.task_id)
        if worker is None:
            logging.error(f"cannot find task_id={task_cancel.task_id.hex()} in task workers")
            return

        await self._binder.send(worker, TaskCancel.new_msg(task_cancel.task_id))

    async def on_task_result(self, task_result: TaskResult):
        worker = self._allocator_policy.remove_task(task_result.task_id)

        if task_result.status in {TaskStatus.Canceled, TaskStatus.NotFound}:
            if worker is not None:
                # The worker canceled the task, but the scheduler still had it queued. Re-route the task to another
                # worker.
                await self.__reroute_tasks([task_result.task_id])
            else:
                await self._task_manager.on_task_done(task_result)

            return

        if worker is None:
            logging.error(
                f"received unknown task result for task_id={task_result.task_id.hex()}, status={task_result.status} "
                f"might due to worker get disconnected or canceled"
            )
            return

        await self._task_manager.on_task_done(task_result)

    async def on_heartbeat(self, worker: bytes, info: WorkerHeartbeat):
        # TODO: get worker queue size from worker heartbeat
        if await self._allocator_policy.add_worker(worker, DEFAULT_PER_WORKER_QUEUE_SIZE):
            logging.info(f"worker {worker!r} connected")
            await self._binder_monitor.send(StateWorker.new_msg(worker, b"connected"))

        self._worker_alive_since[worker] = (time.time(), info)
        await self._binder.send(worker, WorkerHeartbeatEcho.new_msg())

    async def on_client_shutdown(self, client: bytes):
        for worker in self._allocator_policy.get_worker_ids():
            await self.__shutdown_worker(worker)

    async def on_disconnect(self, source: bytes, request: DisconnectRequest):
        await self.__disconnect_worker(request.worker)
        await self._binder.send(source, DisconnectResponse.new_msg(request.worker))

    async def routine(self):
        await self.__clean_workers()

    def get_status(self) -> WorkerManagerStatus:
        worker_to_task_numbers = self._allocator_policy.statistics()
        return WorkerManagerStatus.new_msg(
            [
                self.__worker_status_from_heartbeat(worker, worker_to_task_numbers[worker], last, info)
                for worker, (last, info) in self._worker_alive_since.items()
            ]
        )

    @staticmethod
    def __worker_status_from_heartbeat(
        worker: bytes, worker_task_numbers: Dict, last: float, info: WorkerHeartbeat
    ) -> WorkerStatus:
        current_processor = next((p for p in info.processors if not p.suspended), None)
        suspended = min(len([p for p in info.processors if p.suspended]), UINT8_MAX)
        last_s = min(int(time.time() - last), UINT8_MAX)

        if current_processor:
            debug_info = f"{int(current_processor.initialized)}{int(current_processor.has_task)}{int(info.task_lock)}"
        else:
            debug_info = f"00{int(info.task_lock)}"

        return WorkerStatus.new_msg(
            worker_id=worker,
            agent=info.agent,
            rss_free=info.rss_free,
            free=worker_task_numbers["free"],
            sent=worker_task_numbers["sent"],
            queued=info.queued_tasks,
            suspended=suspended,
            lag_us=info.latency_us,
            last_s=last_s,
            itl=debug_info,
            processor_statuses=[
                ProcessorStatus.new_msg(
                    pid=p.pid,
                    initialized=p.initialized,
                    has_task=p.has_task,
                    suspended=p.suspended,
                    resource=Resource.new_msg(p.resource.cpu, p.resource.rss),
                )
                for p in info.processors
            ],
        )

    def has_available_worker(self) -> bool:
        return self._allocator_policy.has_available_worker()

    def get_worker_by_task_id(self, task_id: bytes) -> bytes:
        return self._allocator_policy.get_worker_by_task_id(task_id)

    def get_worker_ids(self) -> Set[bytes]:
        return self._allocator_policy.get_worker_ids()

    async def __clean_workers(self):
        now = time.time()
        dead_workers = [
            dead_worker
            for dead_worker, (alive_since, info) in self._worker_alive_since.items()
            if now - alive_since > self._timeout_seconds
        ]
        for dead_worker in dead_workers:
            await self.__disconnect_worker(dead_worker)

    async def __reroute_tasks(self, task_ids: List[bytes]):
        for task_id in task_ids:
            await self._task_manager.on_task_reroute(task_id)

    async def __disconnect_worker(self, worker: bytes):
        """return True if disconnect worker success"""
        if worker not in self._worker_alive_since:
            return

        logging.info(f"worker {worker!r} disconnected")
        await self._binder_monitor.send(StateWorker.new_msg(worker, b"disconnected"))
        self._worker_alive_since.pop(worker)

        task_ids = self._allocator_policy.remove_worker(worker)
        if not task_ids:
            return

        logging.info(f"rerouting {len(task_ids)} tasks")
        await self.__reroute_tasks(task_ids)

    async def __shutdown_worker(self, worker: bytes):
        await self._binder.send(worker, ClientDisconnect.new_msg(ClientDisconnect.DisconnectType.Shutdown))
        await self.__disconnect_worker(worker)
