import functools
import logging
import os
import threading
import uuid
from collections import Counter
from concurrent.futures import Future
from inspect import signature
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import zmq
import zmq.asyncio

from scaler.client.agent.client_agent import ClientAgent
from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.future import ScalerFuture
from scaler.client.object_buffer import ObjectBuffer
from scaler.client.object_reference import ObjectReference
from scaler.client.serializer.default import DefaultSerializer
from scaler.client.serializer.mixins import Serializer
from scaler.io.config import DEFAULT_CLIENT_TIMEOUT_SECONDS, DEFAULT_HEARTBEAT_INTERVAL_SECONDS
from scaler.io.sync_connector import SyncConnector
from scaler.protocol.python.message import (
    Argument,
    ArgumentType,
    ClientDisconnect,
    ClientShutdownResponse,
    DisconnectType,
    GraphTask,
    Task,
)
from scaler.utility.exceptions import ClientQuitException
from scaler.utility.graph.optimization import cull_graph
from scaler.utility.graph.topological_sorter import TopologicalSorter
from scaler.utility.metadata.profile_result import ProfileResult
from scaler.utility.metadata.task_flags import TaskFlags, retrieve_task_flags_from_task
from scaler.utility.zmq_config import ZMQConfig, ZMQType
from scaler.worker.agent.processor.processor import Processor


class Client:
    def __init__(
        self,
        address: str,
        profiling: bool = False,
        timeout_seconds: int = DEFAULT_CLIENT_TIMEOUT_SECONDS,
        heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        serializer: Serializer = DefaultSerializer(),
    ):
        """
        The Scaler Client used to send tasks to a scheduler.

        :param address: Address of Scheduler to submit work to
        :type address: str
        :param profiling: If True, the returned futures will have the `task_duration()` property enabled.
        :type profiling: bool
        :param timeout_seconds: Seconds until heartbeat times out
        :type timeout_seconds: int
        :param heartbeat_interval_seconds: Frequency of heartbeat to scheduler in seconds
        :type heartbeat_interval_seconds: int
        """
        self._serializer = serializer

        self._profiling = profiling
        self._identity = f"{os.getpid()}|Client|{uuid.uuid4().bytes.hex()}".encode()

        self._client_agent_address = ZMQConfig(ZMQType.inproc, host=f"scaler_client_{uuid.uuid4().hex}")
        self._scheduler_address = ZMQConfig.from_string(address)
        self._timeout_seconds = timeout_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds

        self._stop_event = threading.Event()
        self._internal_context = zmq.Context()
        self._connector = SyncConnector(
            context=self._internal_context,
            socket_type=zmq.PAIR,
            address=self._client_agent_address,
            identity=self._identity,
        )

        self._future_manager = ClientFutureManager(self._serializer)
        self._agent = ClientAgent(
            identity=self._identity,
            client_agent_address=self._client_agent_address,
            scheduler_address=ZMQConfig.from_string(address),
            context=self._internal_context,
            future_manager=self._future_manager,
            stop_event=self._stop_event,
            timeout_seconds=self._timeout_seconds,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            serializer=self._serializer,
        )
        self._agent.start()

        logging.info(f"ScalerClient: connect to {self._scheduler_address.to_address()}")

        self._object_buffer = ObjectBuffer(self._identity, self._serializer, self._connector)
        self._future_factory = functools.partial(ScalerFuture, connector=self._connector)

        self._object_buffer.buffer_send_serializer()
        self._object_buffer.commit_send_objects()

    @property
    def identity(self):
        return self._identity

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __getstate__(self) -> dict:
        """
        Serializes the client object's state.

        Client serialization is useful when a client reference is used within a remote task:


        .. code:: python

            client = Client(...)

            def fibonacci(client: Client, n: int):
                if n == 0:
                    return 0
                elif n == 1:
                    return 1
                else:
                    a = client.submit(fibonacci, n - 1)
                    b = client.submit(fibonacci, n - 2)
                    return a.result() + b.result()

            print(client.submit(fibonacci, client, 7).result())


        When serializing the client, only saves the address parameters. When deserialized, a new client object
        connecting to the same scheduler and remote logger will be instantiated.
        """

        return {
            "address": self._scheduler_address.to_address(),
            "profiling": self._profiling,
            "timeout_seconds": self._timeout_seconds,
            "heartbeat_interval_seconds": self._heartbeat_interval_seconds,
        }

    def __setstate__(self, state: dict) -> None:
        self.__init__(
            address=state["address"],
            profiling=state["profiling"],
            timeout_seconds=state["timeout_seconds"],
            heartbeat_interval_seconds=state["heartbeat_interval_seconds"],
        )

    def submit(self, fn: Callable, *args, **kwargs) -> ScalerFuture:
        """
        Submit a single task (function with arguments) to the scheduler, and return a future

        :param fn: function to be executed remotely
        :type fn: Callable
        :param args: positional arguments will be passed to function
        :return: future of the submitted task
        :rtype: ScalerFuture
        """

        self.__assert_client_not_stopped()

        function_object_id = self._object_buffer.buffer_send_function(fn).object_id
        all_args = Client.__convert_kwargs_to_args(fn, args, kwargs)

        task, future = self.__submit(function_object_id, all_args, delayed=True)

        self._object_buffer.commit_send_objects()
        self._connector.send(task)
        return future

    def map(self, fn: Callable, iterable: Iterable[Tuple[Any, ...]]) -> List[Any]:
        if not all(isinstance(args, (tuple, list)) for args in iterable):
            raise TypeError("iterable should be list of arguments(list or tuple-like) of function")

        self.__assert_client_not_stopped()

        function_object_id = self._object_buffer.buffer_send_function(fn).object_id
        tasks, futures = zip(*[self.__submit(function_object_id, args, delayed=False) for args in iterable])

        self._object_buffer.commit_send_objects()
        for task in tasks:
            self._connector.send(task)

        try:
            results = [fut.result() for fut in futures]
        except Exception as e:
            logging.exception(f"error happened when do scaler client.map:\n{e}")
            self.disconnect()
            raise e

        return results

    def get(
        self, graph: Dict[str, Union[Any, Tuple[Union[Callable, Any], ...]]], keys: List[str], block: bool = True
    ) -> Dict[str, Union[Any, Future]]:
        """
        .. code-block:: python
           :linenos:
            graph = {
                "a": 1,
                "b": 2,
                "c": (inc, "a"),
                "d": (inc, "b"),
                "e": (add, "c", "d")
            }

        :param graph: dictionary presentation of task graphs
        :type graph: Dict[str, Union[Any, Tuple[Union[Callable, Any]]
        :param keys: list of keys want to get results from computed graph
        :type keys: List[str]
        :param block: if True, it will directly return a dictionary that maps from keys to results
        :return: dictionary of mapping keys to futures, or map to results if block=True is specified
        :rtype: Dict[ScalerFuture]
        """

        self.__assert_client_not_stopped()

        graph = cull_graph(graph, keys)

        node_name_to_argument, graph = self.__split_data_and_graph(graph)
        self.__check_graph(node_name_to_argument, graph, keys)

        graph, compute_futures, finished_futures = self.__construct_graph(node_name_to_argument, graph, keys, block)
        self._object_buffer.commit_send_objects()
        self._connector.send(graph)

        self._future_manager.add_future(
            self._future_factory(
                task=Task(graph.task_id, self._identity, b"", b"", []), is_delayed=not block, group_task_id=None
            )
        )
        for future in compute_futures.values():
            self._future_manager.add_future(future)

        # preserve the future insertion order based on inputted keys
        futures = {}
        for key in keys:
            if key in compute_futures:
                futures[key] = compute_futures[key]
            else:
                futures[key] = finished_futures[key]

        if not block:
            # just return futures
            return futures

        try:
            results = {k: v.result() for k, v in futures.items()}
        except Exception as e:
            logging.exception(f"error happened when do scaler client.get:\n{e}")
            self.disconnect()
            raise e

        return results

    def send_object(self, obj: Any, name: Optional[str] = None) -> ObjectReference:
        """
        send object to scheduler, this can be used to cache very large data to scheduler, and reuse it in multiple
        tasks

        :param obj: object to send, it will be serialized and send to scheduler
        :type obj: Any
        :param name: give a name to the cached argument
        :type name: Optional[str]
        :return: object reference
        :rtype ObjectReference
        """

        self.__assert_client_not_stopped()

        cache = self._object_buffer.buffer_send_object(obj, name)
        return ObjectReference(cache.object_name, cache.object_id, len(cache.object_bytes))

    def disconnect(self):
        """
        disconnect from connected scheduler, this will not shut down the scheduler
        """

        if self._stop_event.is_set():
            self.__destroy()
            return

        logging.info(f"ScalerClient: disconnect from {self._scheduler_address.to_address()}")

        self._future_manager.cancel_all_futures()

        self._connector.send(ClientDisconnect(DisconnectType.Disconnect))

        self.__destroy()

    def __receive_shutdown_response(self):
        message = None
        while not isinstance(message, ClientShutdownResponse):
            message = self._connector.receive()

        if not message.accepted:
            raise ValueError("Scheduler is in protected mode. Can't shutdown")

    def shutdown(self):
        """
        shutdown all workers that connected to the scheduler this client connects to, it will cancel all other
        clients' ongoing tasks, please be aware shutdown might not success if scheduler is configured as protected mode,
        then it cannot shut down scheduler and the workers
        """

        if not self._agent.is_alive():
            self.__destroy()
            return

        logging.info(f"ScalerClient: request shutdown for {self._scheduler_address.to_address()}")

        self._future_manager.cancel_all_futures()

        self._connector.send(ClientDisconnect(DisconnectType.Shutdown))
        try:
            self.__receive_shutdown_response()
        finally:
            self.__destroy()

    def __submit(self, function_object_id: bytes, args: Tuple[Any, ...], delayed: bool) -> Tuple[Task, ScalerFuture]:
        task_id = uuid.uuid1().bytes

        object_ids = []
        for arg in args:
            if isinstance(arg, ObjectReference):
                object_ids.append(arg.object_id)
            else:
                object_ids.append(self._object_buffer.buffer_send_object(arg).object_id)

        task_flags_bytes = self.__get_task_flags().serialize()

        arguments = [Argument(ArgumentType.ObjectID, object_id) for object_id in object_ids]
        task = Task(task_id, self._identity, task_flags_bytes, function_object_id, arguments)

        future = self._future_factory(task=task, is_delayed=delayed, group_task_id=None)
        self._future_manager.add_future(future)
        return task, future

    @staticmethod
    def __convert_kwargs_to_args(fn: Callable, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple[Any, ...]:
        all_params = [p for p in signature(fn).parameters.values()]

        params = [p for p in all_params if p.kind in {p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD}]

        if len(args) >= len(params):
            return args

        number_of_required = len([p for p in params if p.default is p.empty])

        args = list(args)
        kwargs = kwargs.copy()
        kwargs.update({p.name: p.default for p in all_params if p.kind == p.KEYWORD_ONLY if p.default != p.empty})

        for p in params[len(args) : number_of_required]:
            try:
                args.append(kwargs.pop(p.name))
            except KeyError:
                missing = tuple(p.name for p in params[len(args) : number_of_required])
                raise TypeError(f"{fn} missing {len(missing)} arguments: {missing}")

        for p in params[len(args) :]:
            args.append(kwargs.pop(p.name, p.default))

        return tuple(args)

    def __split_data_and_graph(
        self, graph: Dict[str, Union[Any, Tuple[Union[Callable, Any], ...]]]
    ) -> Tuple[Dict[str, Tuple[Argument, Any]], Dict[str, Tuple[Union[Callable, Any], ...]]]:
        graph = graph.copy()
        node_name_to_argument = {}

        for node_name, node in graph.items():
            if isinstance(node, tuple) and len(node) > 0 and callable(node[0]):
                continue

            if isinstance(node, ObjectReference):
                node_name_to_argument[node_name] = (Argument(ArgumentType.ObjectID, node.object_id), None)
            else:
                object_cache = self._object_buffer.buffer_send_object(node, name=node_name)
                node_name_to_argument[node_name] = (Argument(ArgumentType.ObjectID, object_cache.object_id), node)

        for node_name in node_name_to_argument.keys():
            graph.pop(node_name)

        return node_name_to_argument, graph

    @staticmethod
    def __check_graph(
        node_to_argument: Dict[str, Tuple[Argument, Any]],
        graph: Dict[str, Union[Any, Tuple[Union[Callable, Any], ...]]],
        keys: List[str],
    ):
        duplicate_keys = [key for key, count in dict(Counter(keys)).items() if count > 1]
        if duplicate_keys:
            raise KeyError(f"duplicate key detected in argument keys: {duplicate_keys}")

        # sanity check graph
        for key in keys:
            if key not in graph and key not in node_to_argument:
                raise KeyError(f"key {key} has to be in graph")

        sorter = TopologicalSorter()
        for node_name, node in graph.items():
            assert (
                isinstance(node, tuple) and len(node) > 0 and callable(node[0])
            ), "node has to be tuple and first item should be function"

            for arg in node[1:]:
                if arg not in node_to_argument and arg not in graph:
                    raise KeyError(f"argument {arg} in node '{node_name}': {tuple(node)} is not defined in graph")

            sorter.add(node_name, *node[1:])

        # check cyclic dependencies
        sorter.prepare()

    def __construct_graph(
        self,
        node_name_to_arguments: Dict[str, Tuple[Argument, Any]],
        graph: Dict[str, Tuple[Union[Callable, Any], ...]],
        keys: List[str],
        block: bool,
    ) -> Tuple[GraphTask, Dict[str, ScalerFuture], Dict[str, ScalerFuture]]:
        graph_task_id = uuid.uuid1().bytes

        node_name_to_task_id = {node_name: uuid.uuid1().bytes for node_name in graph.keys()}

        task_flags_bytes = self.__get_task_flags().serialize()

        task_id_to_tasks = dict()

        for node_name, node in graph.items():
            task_id = node_name_to_task_id[node_name]

            function, *args = node
            function_cache = self._object_buffer.buffer_send_function(function)

            arguments = []
            for arg in args:
                assert arg in graph or arg in node_name_to_arguments

                if arg in graph:
                    argument = Argument(ArgumentType.Task, node_name_to_task_id[arg])
                else:
                    argument, _ = node_name_to_arguments[arg]

                arguments.append(argument)

            task_id_to_tasks[task_id] = Task(
                task_id, self._identity, task_flags_bytes, function_cache.object_id, arguments
            )

        result_task_ids = [node_name_to_task_id[key] for key in keys if key in graph]
        graph_task = GraphTask(graph_task_id, self._identity, result_task_ids, list(task_id_to_tasks.values()))

        compute_futures = {}
        ready_futures = {}
        for key in keys:
            if key in graph:
                future = self._future_factory(
                    task=task_id_to_tasks[node_name_to_task_id[key]], is_delayed=not block, group_task_id=graph_task_id
                )
                compute_futures[key] = future

            elif key in node_name_to_arguments:
                argument, data = node_name_to_arguments[key]
                future: ScalerFuture = self._future_factory(
                    task=Task(argument.data, self._identity, b"", b"", []),
                    is_delayed=False,
                    group_task_id=graph_task_id,
                )
                future.set_result_ready(argument.data, ProfileResult())
                future.set_result(data)
                ready_futures[key] = future

            else:
                raise ValueError(f"cannot find {key=} in graph")

        return graph_task, compute_futures, ready_futures

    def __get_task_flags(self) -> TaskFlags:
        parent_task_priority = self.__get_parent_task_priority()

        if parent_task_priority is not None:
            task_priority = parent_task_priority - 1
        else:
            task_priority = 0

        return TaskFlags(profiling=self._profiling, priority=task_priority)

    @staticmethod
    def __get_parent_task_priority() -> Optional[int]:
        """If the client is running inside a Scaler processor, returns the priority of the associated task."""

        current_processor = Processor.get_current_processor()

        if current_processor is None:
            return None

        current_task = current_processor.current_task()
        assert current_task is not None

        return retrieve_task_flags_from_task(current_task).priority

    def __assert_client_not_stopped(self):
        if self._stop_event.is_set():
            raise ClientQuitException("client is already stopped.")

    def __destroy(self):
        self._agent.join()
        self._internal_context.destroy(linger=1)