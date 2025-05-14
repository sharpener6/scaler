import json
import logging
from typing import Optional

import psutil

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.message import StateScheduler, InformationCommand, InformationCommandResponse
from scaler.protocol.python.status import Resource
from scaler.scheduler.managers.mixins import (
    ClientManager,
    ObjectManager,
    TaskManager,
    WorkerManager,
    InformationManager,
)
from scaler.utility.mixins import Looper

UNKNOWN_COMMAND_RESPONSE = b'{"error": "unknown command"}'


class VanillaInformationManager(InformationManager, Looper):
    def __init__(self, binder: AsyncConnector):
        self._monitor_binder: AsyncConnector = binder
        self._process = psutil.Process()

        self._binder: Optional[AsyncBinder] = None
        self._client_manager: Optional[ClientManager] = None
        self._object_manager: Optional[ObjectManager] = None
        self._task_manager: Optional[TaskManager] = None
        self._worker_manager: Optional[WorkerManager] = None

    def register_managers(
        self,
        binder: AsyncBinder,
        client_manager: ClientManager,
        object_manager: ObjectManager,
        task_manager: TaskManager,
        worker_manager: WorkerManager,
    ):
        self._binder = binder
        self._client_manager = client_manager
        self._object_manager = object_manager
        self._task_manager = task_manager
        self._worker_manager = worker_manager

    async def on_command(self, source: bytes, command: InformationCommand):
        command_str = command.command.decode()
        command_tuple = command_str.split(" ")

        response = None
        if command_tuple[0] == "task":
            if command_tuple[1] == "all":
                response = json.dumps(self._task_manager.get_task_paths(set()))

        if response is None:
            logging.info(f"Source[{source.hex()}]: unknown information command '{command_str}'")
            response = UNKNOWN_COMMAND_RESPONSE

        await self._binder.send(source, InformationCommandResponse.new_msg(response=response))

    async def routine(self):
        await self._monitor_binder.send(
            StateScheduler.new_msg(
                binder=self._binder.get_status(),
                scheduler=Resource.new_msg(int(self._process.cpu_percent() * 10), self._process.memory_info().rss),
                rss_free=psutil.virtual_memory().available,
                client_manager=self._client_manager.get_status(),
                object_manager=self._object_manager.get_status(),
                task_manager=self._task_manager.get_status(),
                worker_manager=self._worker_manager.get_status(),
            )
        )
