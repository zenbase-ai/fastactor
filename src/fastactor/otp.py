from abc import ABC
import asyncio
from collections import deque
from dataclasses import dataclass, field
from time import monotonic
import logging
import typing as t

from anyio import (
    ClosedResourceError,
    EndOfStream,
    create_memory_object_stream,
    create_task_group,
    Event,
    fail_after,
    Lock,
    TASK_STATUS_IGNORED,
)
from anyio.abc import TaskGroup, TaskStatus
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream

from beartype import beartype
from sorcery import dict_of

# Adjust these to your actual import paths
from .settings import settings
from .utils import id_generator

# -----------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------
# Exceptions
# -----------------------------------------------------------
class ActorCrashed(Exception):
    pass


class ActorFailed(Exception):
    pass


class SystemNotAvailable(RuntimeError):
    pass


class SystemAlreadyActive(RuntimeError):
    pass


# -----------------------------------------------------------
# Helpers
# -----------------------------------------------------------
def _is_normal_shutdown_reason(reason: t.Any) -> bool:
    if reason in ("normal", "shutdown"):
        return True
    if isinstance(reason, tuple) and len(reason) > 0 and reason[0] == "shutdown":
        return True
    return False


# -----------------------------------------------------------
# Base Message Types
# -----------------------------------------------------------
@dataclass
class Message(ABC):
    sender: "Process"


@dataclass
class Info(Message):
    message: t.Any


@dataclass
class Stop(Message):
    reason: t.Any
    reply: t.Optional[t.Any] = None


@dataclass
class Exit(Message):
    reason: t.Any


@dataclass
class Down(Message):
    reason: t.Any


class Ignore:
    """Used by `Process.init` to skip starting the loop if desired."""


# -----------------------------------------------------------
# Minimal Actor Process
# -----------------------------------------------------------
@dataclass(repr=False)
class Process:
    """
    A minimal actor with mailbox, linking/monitoring, start/stop lifecycle.
    Does NOT implement synchronous call/cast logic; that belongs in `GenServer`.
    """

    id: str = field(default_factory=id_generator("process"))
    supervisor: t.Optional["Supervisor"] = None
    trap_exits: bool = False

    _inbox: MemoryObjectSendStream[Message] | None = field(default=None, init=False)
    _mailbox: MemoryObjectReceiveStream[Message] | None = field(
        default=None,
        init=False,
    )

    _started: Event = field(default_factory=Event, init=False)
    _stopped: Event = field(default_factory=Event, init=False)
    _crash_exc: Exception = field(default=None, init=False)

    links: set["Process"] = field(default_factory=set, init=False)
    monitors: set["Process"] = field(default_factory=set, init=False)
    monitored_by: set["Process"] = field(default_factory=set, init=False)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other: "Process"):
        return self.id == other.id

    # --------------- Lifecycle --------------- #
    async def _init(self, *args, **kwargs):
        logger.debug("%s init", self)
        try:
            init_result = await self.init(*args, **kwargs)
        except Exception as error:
            logger.error("%s init crashed %s", self, error)
            init_result = Stop(self, error)

        match init_result:
            case Ignore():
                self._started.set()
                self._stopped.set()
            case Stop(_, reason):
                self._started.set()
                self._stopped.set()
                raise RuntimeError(f"Process init stopped: {reason}")
            case _:
                self._inbox, self._mailbox = create_memory_object_stream(
                    settings.mailbox_size
                )

    async def init(self, *args, **kwargs) -> t.Union[Ignore, Stop, None]:
        """
        Subclasses should override. Return Ignore to skip the loop,
        or Stop(...) to fail initialization.
        """
        return None

    def has_started(self) -> bool:
        return self._started.is_set()

    async def started(self):
        await self._started.wait()

    def has_stopped(self) -> bool:
        return self._stopped.is_set()

    async def stopped(self):
        await self._stopped.wait()

    async def handle_exit(self, message: Exit):
        """Override in subclass if needed."""
        pass

    async def terminate(self, reason: t.Any):
        """
        Called after the main loop ends or if forcibly stopped.
        Subclasses can override for final cleanup.
        """
        logger.debug("%s terminate %s", self, reason)

        # Notify monitors about our exit
        for process in list(self.monitors):
            try:
                await process.send(Down(self, reason))
            except Exception as error:
                logger.error(
                    "%r sending Down(%s, %s) to monitor: %s",
                    error,
                    self,
                    reason,
                    process,
                )

        abnormal_shutdown = not _is_normal_shutdown_reason(reason)
        for process in list(self.links):
            if process.trap_exits:
                try:
                    await process.send(Exit(self, reason))
                except Exception as error:
                    logger.error(
                        "%r sending Exit(%s, %s) to linked actor: %s",
                        error,
                        self,
                        reason,
                        process,
                    )
            elif abnormal_shutdown:
                process._crash_exc = (
                    reason
                    if isinstance(reason, Exception)
                    else ActorCrashed(str(reason))
                )
                await process.kill()

        for process in list(self.links):
            process.unlink(self)
        for process in list(self.monitors):
            process.demonitor(self)
        for process in list(self.monitored_by):
            process.demonitor(self)

        Runtime.current().unregister(self)

    # --------------- Link / Monitor --------------- #
    def link(self, other: "Process"):
        self.links.add(other)
        other.links.add(self)

    def unlink(self, other: "Process"):
        self.links.discard(other)
        other.links.discard(self)

    def monitor(self, other: "Process"):
        self.monitors.add(other)
        other.monitored_by.add(self)

    def demonitor(self, other: "Process"):
        self.monitors.discard(other)
        other.monitored_by.discard(self)

    # --------------- Basic Send / Stop --------------- #
    async def send(self, message: t.Any):
        await self._inbox.send(message)

    def send_nowait(self, message: t.Any):
        self._inbox.send_nowait(message)

    async def stop(
        self,
        reason: t.Any = "normal",
        timeout: t.Optional[int] = 60,
        sender: t.Optional["Process"] = None,
    ):
        """
        Gracefully stop by sending a Stop(...) message.
        """
        logger.debug("%s stop %s", self, reason)
        self.send_nowait(Stop(sender or self.supervisor, reason))
        with fail_after(timeout):
            await self.stopped()

    async def kill(self):
        """
        Forcibly kill the process by closing the mailbox.
        """
        logger.debug("%s kill", self)
        await self._mailbox.aclose()
        await self.stopped()

    def info(self, message: t.Any, sender: t.Optional["Process"] = None):
        sender = sender or self.supervisor
        self.send_nowait(Info(sender, message))

    async def handle_info(self, message: Message): ...

    # --------------- Internal Loop --------------- #
    async def _loop(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        """
        The main loop: reads messages, calls `_handle_message`.
        """
        task_status.started()
        self._started.set()
        logger.debug("%s loop started", self)
        while True:
            try:
                message = await self._mailbox.receive()
                logger.debug("%s received message %s", self, message)
            except (EndOfStream, ClosedResourceError):
                break

            keep_going = await self._handle_message(message)
            if not keep_going:
                break

    async def _handle_message(self, message: Message) -> bool:
        """
        Returns True to keep going, False to break the loop.
        """
        match message:
            case Stop():
                # We are told to stop
                return False
            case Exit():
                await self.handle_exit(message)
                return True
            case _:
                # Unrecognized => treat as info by default
                await self.handle_info(message)
                return True

    async def loop(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        """
        Wraps `_loop` in a try/finally to detect crashes. Subclasses can override or extend.
        """
        async with self._mailbox:
            reason = "normal"
            try:
                await self._loop(task_status=task_status)
            except Exception as error:
                self._crash_exc = error
                reason = error
            finally:
                await self.terminate(reason)
                self._stopped.set()

    @classmethod
    async def start(
        cls,
        *args,
        trap_exits=False,
        supervisor: t.Optional["Supervisor"] = None,
        **kwargs,
    ) -> t.Optional[t.Self]:
        runtime = Runtime.current()
        supervisor = supervisor or runtime.supervisor

        process = cls(supervisor=supervisor, trap_exits=trap_exits)
        return await runtime.spawn(process, *args, **kwargs)

    @classmethod
    async def start_link(
        cls,
        *args,
        trap_exits=False,
        supervisor: t.Optional["Supervisor"] = None,
        **kwargs,
    ) -> t.Optional[t.Self]:
        """
        Similar to `start`, but also link the new process with the supervisor.
        """
        process = await cls.start(
            *args, trap_exits=trap_exits, supervisor=supervisor, **kwargs
        )
        if process is not None:
            process.link(process.supervisor)
            return process


# -----------------------------------------------------------
# GenServer
# -----------------------------------------------------------
@dataclass
class Call(Message):
    """
    GenServer-specific synchronous call.
    """

    message: t.Any
    _result: t.Any = field(default=None, init=False)
    _ready: Event = field(default_factory=Event, init=False, repr=False)

    def set_result(self, value: t.Any):
        self._result = value
        self._ready.set()

    async def result(self, timeout=5):
        with fail_after(timeout):
            await self._ready.wait()
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


@dataclass
class Cast(Message):
    message: t.Any


class GenServer(Process):
    """
    A GenServer extends Process with `call`/`cast` semantics similar to Elixir's GenServer.
    """

    # Overridable callbacks
    async def handle_call(self, sender: "Process", request: t.Any) -> t.Any:
        return None

    async def handle_cast(self, sender: "Process", request: t.Any) -> t.Any:
        pass

    async def handle_info(self, sender: "Process", message: t.Any):
        pass

    # Public API for user code
    async def call(
        self,
        request: t.Any,
        sender: t.Optional["Process"] = None,
        timeout=5,
    ) -> t.Any:
        sender = sender or self.supervisor
        callmsg = Call(sender, request)
        self.send_nowait(callmsg)
        return await callmsg.result(timeout)

    def cast(self, request: t.Any, sender: t.Optional["Process"] = None):
        sender = sender or self.supervisor
        self.send_nowait(Cast(sender, request))

    async def _handle_message(self, message: t.Any) -> bool:
        """
        We override to check for call/cast messages. If it's not, fallback to base `_handle_message`.
        """
        match message:
            case Call():
                try:
                    reply = await self.handle_call(message)
                    message.set_result(reply)
                    return True
                except Exception as error:
                    message.set_result(error)
                    logger.error(
                        "%s encountered %r processing %s", self, error, message
                    )
                    raise error
            case Cast():
                try:
                    await self.handle_cast(message)
                    return True
                except Exception as error:
                    logger.error(
                        "%s encountered %r processing %s", self, error, message
                    )
                    raise error
            case _:
                return await super()._handle_message(message)


# -----------------------------------------------------------
# Supervisor
# -----------------------------------------------------------
RestartType = t.Literal["permanent", "transient", "temporary"]
ShutdownType = t.Union[int, t.Literal["brutal_kill", "infinity"]]
RestartStrategy = t.Literal["one_for_one", "one_for_all"]


class ChildSpec(t.NamedTuple):
    type: type[Process]
    args: tuple
    kwargs: dict
    restart_type: RestartType
    shutdown_type: ShutdownType


class Child(t.NamedTuple):
    child_id: str
    child_proc: Process
    child_type: str
    child_modules: list[type[Process]]


class RunningChild(t.NamedTuple):
    process: Process
    restart_type: RestartType
    shutdown_type: ShutdownType


@dataclass(repr=False)
class Supervisor(Process):
    child_specs: dict[str, ChildSpec] = field(default_factory=dict)
    strategy: RestartStrategy = "one_for_one"
    max_restarts: int = 3
    max_seconds: float = 5.0
    trap_exits: bool = True

    children: dict[str, RunningChild] = field(default_factory=dict, init=False)
    _task_group: TaskGroup | None = field(default=None, init=False)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other: "Supervisor"):
        return self.id == other.id

    async def init(self, *args, **kwargs):
        # Called once at supervisor startup
        # Usually returns None => normal start
        return None

    async def terminate(self, reason: t.Any):
        """
        Stop all children on termination.
        """
        tasks = [
            self._shutdown_child(actor, sh, reason)
            for actor, _, sh in list(self.children.values())
        ]
        await asyncio.gather(*tasks)
        self.children.clear()

        await super().terminate(reason)

    def _should_restart(self, reason: t.Any, rst: RestartType) -> bool:
        if rst == "permanent":
            return True
        elif rst == "temporary":
            return False
        elif rst == "transient":
            return not _is_normal_shutdown_reason(reason)
        raise ValueError(f"Unsupported restart type: {rst}")

    async def _shutdown_child(
        self, actor: Process, shutdown: ShutdownType, reason="normal"
    ):
        if actor.has_stopped():
            return
        elif shutdown == "brutal_kill":
            await actor.kill()
            await actor.stopped()
        elif shutdown == "infinity":
            await actor.stop(reason, timeout=None, sender=self)
        elif isinstance(shutdown, int):
            await actor.stop(reason, timeout=shutdown, sender=self)
        else:
            raise ValueError(f"Unsupported shutdown type: {shutdown}")

    async def _run_child(
        self,
        child_id: str,
        child_spec: ChildSpec,
        *,
        task_status: TaskStatus[None] = TASK_STATUS_IGNORED,
    ):
        restart_times = deque()
        while True:
            process = await child_spec.type.start_link(
                *child_spec.args,
                supervisor=self,
                **child_spec.kwargs,
            )
            self.children[child_id] = RunningChild(
                process,
                child_spec.restart_type,
                child_spec.shutdown_type,
            )
            task_status.started()

            # Wait for the child to stop
            await process.stopped()

            reason = process._crash_exc or "normal"
            if not self._should_restart(reason, child_spec.restart_type):
                break

            now = monotonic()
            restart_times.append(now)
            while restart_times and now - restart_times[0] > self.max_seconds:
                restart_times.popleft()
            if len(restart_times) > self.max_restarts:
                raise ActorFailed("Max restart intensity reached")

        del self.children[child_id]

    async def _one_for_one(self):
        tasks = [
            self._task_group.start(self._run_child, child_id, spec)
            for child_id, spec in self.child_specs.items()
        ]
        await asyncio.gather(*tasks)

    async def _one_for_all(self):
        restarts = deque()
        while True:
            actors_info = []
            # Start them all
            for child_id, spec in self.child_specs.items():
                proc = await spec.type.start_link(
                    *spec.args, supervisor=self, **spec.kwargs
                )
                self.children[child_id] = (proc, spec.restart_type, spec.shutdown_type)
                actors_info.append(
                    (child_id, proc, spec.restart_type, spec.shutdown_type)
                )

            # Wait for them all to stop
            reasons = []
            for _, proc, rst, _ in actors_info:
                await proc.stopped()
                reasons.append((proc._crash_exc or "normal", rst))

            # If any child => abnormal => must restart all
            if any(self._should_restart(r, rst) for (r, rst) in reasons):
                now = monotonic()
                restarts.append(now)
                while restarts and now - restarts[0] > self.max_seconds:
                    restarts.popleft()
                if len(restarts) > self.max_restarts:
                    raise ActorFailed("Max restart intensity reached")
                self.children.clear()
            else:
                self.children.clear()
                break

    async def loop(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        async with create_task_group() as tg:
            self._task_group = tg
            if self.strategy == "one_for_one":
                await self._one_for_one()
            elif self.strategy == "one_for_all":
                await self._one_for_all()
            else:
                raise ValueError("Unsupported strategy")
            await super().loop(task_status=task_status)

    # --- Child management
    def which_children(self) -> list[Child]:
        results = []
        for child_id, spec in self.child_specs.items():
            if child_id in self.children:
                proc, _, _ = self.children[child_id]
                child = proc
            else:
                child = ":undefined"
            ctype = "worker"
            mods = [spec.type]
            results.append(Child(child_id, child, ctype, mods))
        return results

    def count_children(self) -> dict:
        specs = len(self.child_specs)
        active = sum(
            1 for (proc, _, _) in self.children.values() if not proc.has_stopped()
        )
        supervisors = sum(
            1 for spec in self.child_specs.values() if issubclass(spec.type, Supervisor)
        )
        workers = specs - supervisors
        return dict_of(specs, active, supervisors, workers)

    async def terminate_child(self, child_id: str):
        if child_id not in self.children:
            raise RuntimeError(f"Child {child_id} not currently running.")

        child = self.children[child_id]
        await self._shutdown_child(child.process, child.shutdown_type, reason="normal")
        del self.children[child_id]

    def delete_child(self, child_id: str):
        if child_id in self.children:
            raise RuntimeError(
                f"Cannot delete running child {child_id}; terminate it first."
            )

        del self.child_specs[child_id]

    def _check_task_group(self):
        if not self._task_group:
            raise RuntimeError("Supervisor not running")

    async def start_child(
        self,
        child_id: str,
        child_spec: ChildSpec,
    ):
        self._check_task_group()
        if child_id in self.child_specs:
            raise RuntimeError(f"child_id {child_id} already exists")

        self.child_specs[child_id] = child_spec
        await self._task_group.start(self._run_child, child_id, child_spec)
        return child_id

    async def restart_child(self, child_id: str):
        self._check_task_group()
        if child_id not in self.child_specs:
            raise RuntimeError("No such child_id.")
        if child_id in self.children and not self.children[child_id][0].has_stopped():
            raise RuntimeError("Child is running, cannot restart.")

        await self._task_group.start(
            self._run_child, child_id, self.child_specs[child_id]
        )

    @beartype
    @staticmethod
    def child_spec(
        module_or_map: type[Process],
        args: tuple = (),
        kwargs: dict = {},
        restart: RestartType = "permanent",
        shutdown: ShutdownType = 5000,
    ) -> ChildSpec:
        return ChildSpec(module_or_map, args, kwargs, restart, shutdown)


# -----------------------------------------------------------
# Runtime Supervisor
# -----------------------------------------------------------
class RuntimeSupervisor(Supervisor):
    async def handle_info(self, message: t.Any):
        logger.info("RuntimeSupervisor: handle_info %s", message)

    async def handle_exit(self, message: Exit):
        logger.info("RuntimeSupervisor: handle_exit %s", message)


# -----------------------------------------------------------
# Global "Runtime"
# -----------------------------------------------------------
@dataclass(repr=False)
class Runtime(Process):
    # Class variables
    _current: t.ClassVar[t.Optional["Runtime"]] = None
    _lock: t.ClassVar[Lock] = Lock()

    # Instance variables
    supervisor: t.Optional[RuntimeSupervisor] = None
    _task_group: t.Optional[TaskGroup] = None

    # Runtime state
    registry: dict[str, str] = field(default_factory=dict, init=False)
    _reverse_registry: dict[str, str] = field(default_factory=dict, init=False)
    lookup: dict[str, Process] = field(default_factory=dict, init=False)

    @classmethod
    def current(cls) -> "Runtime":
        if cls._current is None:
            raise SystemNotAvailable("No System is currently active.")
        return cls._current

    async def __aenter__(self):
        async with self._lock:
            if Runtime._current is not None:
                raise SystemAlreadyActive("A System is already active.")

            self.supervisor = RuntimeSupervisor(trap_exits=True)
            await self.supervisor._init()

            self._task_group = await create_task_group().__aenter__()
            await self._task_group.start(self.supervisor.loop)

            Runtime._current = self
            return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        async with self._lock:
            if self.supervisor is not None:
                if self.supervisor.has_started() and not self.supervisor.has_stopped():
                    await self.supervisor.stop(sender=self)
                self.supervisor = None

            if self._task_group:
                await self._task_group.__aexit__(exc_type, exc_val, exc_tb)

            Runtime._current = None

    async def spawn[P: Process](self, process: P, *args, **kwargs) -> P:
        await process._init(*args, **kwargs)
        if not process.has_stopped():
            await self._task_group.start(process.loop)

            self.register(process)
            return process

    def register(self, proc: Process):
        self.lookup[proc.id] = proc

    def unregister(self, proc: Process):
        del self.lookup[proc.id]
        if name := self._reverse_registry.get(proc.id):
            self.unregister_name(name)

    def register_name(self, name: str, proc: Process):
        self.registry[name] = proc.id
        self._reverse_registry[proc.id] = name

    def unregister_name(self, name: str):
        id = self.registry.pop(name)
        del self._reverse_registry[id]

    def where_is(self, name: str) -> t.Optional[Process]:
        if id := self.registry.get(name):
            return self.lookup.get(id)
