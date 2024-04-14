from abc import ABC
from dataclasses import dataclass, field
from uuid import uuid4
import asyncio
import logging
import os
import random
import typing as t

from anyio import (
    Event,
    create_memory_object_stream,
    create_task_group,
    fail_after,
)
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from pyee.asyncio import AsyncIOEventEmitter


logger = logging.getLogger(__name__)

LLEGOS_MAILBOX_SIZE = int(os.getenv("LLEGOS_MAILBOX_SIZE", "128"))


def gen_id():
    return str(uuid4())


@dataclass
class Call[T]:
    sender: "Actor"
    request: T
    _response_stream: MemoryObjectSendStream[T]

    id: str = field(default_factory=gen_id)

    def __str__(self):
        return f"Call({self.id})"


@dataclass
class Cast[T]:
    sender: "Actor"
    request: T

    id: str = field(default_factory=gen_id)

    def __str__(self):
        return f"Cast({self.id})"


@dataclass
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

    _started: Event = field(init=False, default_factory=Event)

    async def started(self):
        return await self._started.wait()

    async def init(self):
        """Any async startup code should go here."""
        self.inbox, self.mailbox = create_memory_object_stream(self.mailbox_size)
        self._started.set()

    async def handle_call(self, sender: "Actor", message: Req) -> t.Any:
        """Handle a call message. Return a response."""

    async def handle_cast(self, sender: "Actor", message: Req) -> None:
        """Handle a cast message. No response."""

    async def call[
        Res
    ](self, receiver: "Actor", req: Req, timeout: float | None = 5) -> Res:
        send_stream, receive_stream = create_memory_object_stream(1)

        receiver.inbox.send_nowait(Call(self, req, _response_stream=send_stream))

        with fail_after(timeout):
            response = await receive_stream.receive()

        return response

    async def cast(self, receiver: "Actor", message: Req) -> None:
        await receiver.inbox.send(Cast(self, message))

    async def loop(self):
        await self.init()
        with self.mailbox:
            async for message in self.mailbox:
                await runtime.perform(self, message)


# TODO: Productionization — durable restarts — Redis? Kafka?
class Runtime:
    events: AsyncIOEventEmitter

    def __init__(self) -> None:
        self.events = AsyncIOEventEmitter()

    async def perform(self, actor: Actor, msg: Call | Cast):
        kind = msg.__class__.__name__.lower()
        self.events.emit("before:perform", msg)
        self.events.emit(f"before:{kind}", msg)

        match msg:
            case Call():
                response = await actor.handle_call(msg.sender, msg.request)
                msg._response_stream.send_nowait(response)
            case Cast():
                await actor.handle_cast(msg.sender, msg.request)
                response = None

        self.events.emit(f"after:{kind}", msg, response)
        self.events.emit("after:perform", msg, response)

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
                logger.warning("Actor crashed", exc_info=exc)
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

    children: list[Actor] = field(default_factory=list)
    strategy: t.Callable = field(default=Strategy.one_for_one)
    max_restarts: int = field(default=3)

    async def started(self):
        await asyncio.gather(
            super().started(), *[actor.started() for actor in self.children]
        )

    async def loop(self):
        async with create_task_group() as tg:
            tg.start_soon(super().loop)
            tg.start_soon(
                self.strategy,
                [actor.loop for actor in self.children],
                self.max_restarts,
            )


# Modelled after https://elixirschool.com/en/lessons/misc/poolboy
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
            tg.start_soon(super().loop)

            await self.started()

            async for message in self.mailbox:
                worker = await self.router(message)
                worker.inbox.send_nowait(message)
