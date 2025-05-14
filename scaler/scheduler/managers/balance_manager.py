import logging
from typing import Dict, List, Optional

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.message import StateBalanceAdvice
from scaler.scheduler.allocate_policy.mixins import TaskAllocatePolicy
from scaler.scheduler.managers.mixins import TaskManager
from scaler.utility.mixins import Looper


class VanillaBalanceManager(Looper):
    def __init__(self, load_balance_trigger_times: int, task_allocate_policy: TaskAllocatePolicy):
        self._load_balance_trigger_times = load_balance_trigger_times

        self._task_allocate_policy = task_allocate_policy

        self._last_balance_advice: Dict[bytes, List[bytes]] = dict()
        self._same_load_balance_advice_count = 0

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None

        self._task_manager: Optional[TaskManager] = None

    def register(self, binder: AsyncBinder, binder_monitor: AsyncConnector, task_manager: TaskManager):
        self._binder = binder
        self._binder_monitor = binder_monitor

        self._task_manager = task_manager

    async def routine(self):
        current_advice = self._task_allocate_policy.balance()
        if not self.__should_balance(current_advice):
            return

        worker_to_num_tasks = {worker: len(task_ids) for worker, task_ids in current_advice.items()}
        logging.info(f"balancing task: {worker_to_num_tasks}")
        for worker, task_ids in current_advice.items():
            await self._binder_monitor.send(StateBalanceAdvice.new_msg(worker, task_ids))

        self._last_balance_advice = current_advice
        for worker, task_ids in current_advice.items():
            for task_id in task_ids:
                await self._task_manager.on_task_balance_cancel(task_id)

    def __should_balance(self, current_advice: Dict[bytes, List[bytes]]) -> bool:
        # 1. if this is the same advise as last time, then we +1 on same advice count
        # 2. if there is another different advice come in, then we reset same advice count to 0
        if self._last_balance_advice == current_advice:
            self._same_load_balance_advice_count += 1
        else:
            self._last_balance_advice = current_advice
            self._same_load_balance_advice_count = 0

        # if we have same advice for more than trigger times, then we start doing the balancing
        if 0 < self._same_load_balance_advice_count < self._load_balance_trigger_times:
            return False

        # if current advice is empty, then we skip
        if not current_advice:
            return False

        return True
