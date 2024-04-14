from abc import ABC
from dataclasses import dataclass, field
from uuid import uuid4
import logging
import os
import random
import typing as t

from anyio import create_memory_object_stream, create_task_group, fail_after
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from pyee.asyncio import AsyncIOEventEmitter


logger = logging.getLogger(__name__)

LLEGOS_MAILBOX_SIZE = int(os.getenv("LLEGOS_MAILBOX_SIZE", "128"))


def gen_id():
    return str(uuid4())


@dataclass(frozen=True)
class Call[T]:
    request: T
    _callback_stream: MemoryObjectSendStream[T]

    id: str = field(default_factory=gen_id)

    def __str__(self):
        return f"Call({self.id})"


@dataclass(frozen=True)
class Cast[T]:
    request: T

    id: str = field(default_factory=gen_id)

    def __str__(self):
        return f"Cast({self.id})"


@dataclass(frozen=True)
class Actor[Req](ABC):
    """
    An actor that can handle call and cast messages.

    Call messages wait for a response, while cast messages are fire-and-forget.
    """

    id: str = field(default_factory=gen_id)
    mailbox_size: int = field(default=LLEGOS_MAILBOX_SIZE)
    events: AsyncIOEventEmitter = field(init=False, default_factory=AsyncIOEventEmitter)
    inbox: MemoryObjectSendStream[Req] = field(init=False, default=None)
    mailbox: MemoryObjectReceiveStream[Req] = field(init=False, default=None)

    async def init(self):
        """Any async startup code should go here."""
        self.inbox, self.mailbox = create_memory_object_stream(self.mailbox_size)

    async def handle_call(self, sender: "Actor", message: Req) -> t.Any:
        """Handle a call message. Return a response."""

    async def handle_cast(self, sender: "Actor", message: Req) -> None:
        """Handle a cast message. No response."""

    async def call[
        Res
    ](self, receiver: "Actor", req: Req, timeout: float | None = 5) -> Res:
        send_stream, receive_stream = create_memory_object_stream(1)

        receiver.inbox.send_nowait(Call(req, _callback_stream=send_stream))

        with fail_after(timeout):
            response = await receive_stream.receive()

        return response

    def cast(self, receiver: "Actor", message: Req) -> None:
        receiver.inbox.send_nowait(Cast(message))

    async def loop(self):
        await self.init()
        with self.mailbox:
            async for message in self.mailbox:
                await runtime.perform(self, message)


class Runtime:
    events: AsyncIOEventEmitter

    def __init__(self) -> None:
        self.events = AsyncIOEventEmitter()

    async def perform(self, actor: Actor, message: Call | Cast):
        kind = message.__class__.__name__.lower()
        self.events.emit("before:perform", message)
        self.events.emit(f"before:{kind}", message)

        match message:
            case Call():
                response = await actor.handle_call(message.request)
                message._callback_stream.send_nowait(response)
            case Cast():
                response: None = await actor.handle_cast(message.request)

        self.events.emit(f"after:{kind}", message, response)
        self.events.emit("after:perform", message, response)

        return response


runtime = Runtime()


@dataclass
class Supervisor(Actor):
    class Strategy:
        @classmethod
        async def run(cls, task, max_restarts: int = 3):
            try:
                await task()
            except Exception as exc:
                logger.warning("Actor crashed", exc)
                max_restarts -= 1
                if max_restarts == 0:
                    logger.error("Max restarts reached. Actor will not be restarted.")
                    raise exc

                await cls.run(task, max_restarts)

        @classmethod
        async def one_for_one(cls, tasks: list, max_restarts: int = 3):
            async with create_task_group() as tg:
                for task in tasks:
                    tg.start_soon(cls.run, task, max_restarts)

        @classmethod
        async def one_for_all(cls, tasks: list, max_restarts: int = 3):
            async def run_tasks(tasks):
                async with create_task_group() as tg:
                    for task in tasks:
                        tg.start_soon(task)

            await cls.run(run_tasks, tasks, max_restarts)

    children: list[Actor]
    strategy: t.Callable = field(default=Strategy.one_for_one)
    max_restarts: int = field(defualt=3)

    async def loop(self):
        async with create_task_group() as tg:
            await tg.start(super().loop)
            await tg.start(
                self.strategy,
                [actor.loop for actor in self.children],
                self.max_restarts,
            )


@dataclass
class WorkerPool(Supervisor, ABC):
    """
    A worker pool that distributes messages to workers.
    """

    async def router(self, message: Call | Cast) -> Actor:
        """Select a worker to handle the message."""
        return random.choice(self.children)

    async def loop(self):
        async with create_task_group() as tg:
            await tg.start(super().loop)

            async for message in self.mailbox:
                worker = await self.router(message)
                worker.inbox.send_nowait(message)


# To customize for your own app, import these llegos and subclass them with your own logic.


class AppRuntime(Runtime):
    async def perform(self, actor: Actor, message: Call | Cast):
        response = await super().perform(actor, message)
        print(f"Actor {actor.id} received {message} and responded with {response}")
        return response


runtime = AppRuntime()


class AppActor(Actor):
    async def handle_call(self, sender: Actor, message):
        match message:
            case "ping":
                return "pong"
            case "pong":
                return "ping"


worker_pool = WorkerPool(
    children=[AppActor(), AppActor(), AppActor()],
    strategy=Supervisor.Strategy.one_for_all,
)
