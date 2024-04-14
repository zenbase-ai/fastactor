import random
import typing as t
import logging
from abc import ABC
from dataclasses import dataclass

from anyio import create_memory_object_stream, create_task_group, fail_after
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream


logger = logging.getLogger(__name__)


@dataclass
class Call[T]:
    request: T
    _callback_stream: MemoryObjectSendStream[T]


@dataclass
class Cast[T]:
    request: T


class Actor[Req](ABC):
    """
    An actor that can handle call and cast messages.

    Call messages wait for a response, while cast messages are fire-and-forget.
    """

    inbox: MemoryObjectSendStream[Req]
    mailbox: MemoryObjectReceiveStream[Req]

    def __init__(self, mailbox_size: int = 128):
        self.inbox, self.mailbox = create_memory_object_stream(mailbox_size)

    async def init(self):
        """Any async startup code should go here."""

    async def handle_call(self, sender: "Actor", message: Req):
        """Handle a call message. Return a response."""

    async def handle_cast(self, sender: "Actor", message: Req):
        """Handle a cast message. No response."""

    async def call[
        Res
    ](self, receiver: "Actor", req: Req, timeout: float | None = 5) -> Res:
        send_stream, receive_stream = create_memory_object_stream(1)
        with fail_after(timeout):
            receiver.inbox.send_nowait(Call(req, _callback_stream=send_stream))
            response = await receive_stream.receive()
            return response

    def cast(self, receiver: "Actor", message: Req) -> None:
        receiver.inbox.send_nowait(Cast(message))

    async def loop(self):
        await self.init()
        async with self.mailbox:
            while True:
                async for message in self.mailbox:
                    await process_message(self, message)


async def process_message(actor: Actor, message: Call | Cast):
    match message:
        case Call():
            response = await actor.handle_call(message.request)
            message._callback_stream.send_nowait(response)
        case Cast():
            await actor.handle_cast(message.request)


async def run_task_supervised(task, max_restarts: int = 3):
    try:
        async with create_task_group() as tg:
            tg.start_soon(task)
    except Exception as exc:
        logger.warning("Actor crashed", exc)
        max_restarts -= 1
        if max_restarts == 0:
            logger.error("Max restarts reached. Actor will not be restarted.")
            raise exc

        await run_task_supervised(task, max_restarts)


async def run_tasks_one_for_one(tasks, max_restarts: int = 3):
    async with create_task_group() as tg:
        for task in tasks:
            tg.start_soon(run_task_supervised, task, max_restarts)


async def run_tasks_one_for_all(tasks, max_restarts: int = 3):
    async def run_tasks(tasks):
        async with create_task_group() as tg:
            for task in tasks:
                tg.start_soon(task)

    await run_task_supervised(run_tasks, tasks, max_restarts)


@dataclass
class Supervisor(Actor):
    children: list[Actor]
    run_tasks = run_tasks_one_for_one
    max_restarts: int = 3

    async def loop(self):
        async with create_task_group() as tg:
            tg.start_soon(super().loop)
            tg.start_soon(
                self.run_tasks,
                [actor.loop for actor in self.children],
                self.max_restarts,
            )


@dataclass
class WorkerPool(Supervisor, ABC):
    """
    A worker pool that randomly routes messages to workers.
    """

    async def router(self, message: Call | Cast) -> Actor:
        """Select a worker to handle the message."""
        return random.choice(self.children)

    async def loop(self):
        await self.init()
        async with create_task_group() as tg:
            tg.start_soon(super().loop)
            while True:
                async for message in self.mailbox:
                    worker = await self.router(message)
                    tg.start_soon(process_message, worker, message)
