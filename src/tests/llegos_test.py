import pytest
import typing as t

from anyio import create_task_group

from llegos import llegos


class Pinger(llegos.Actor):
    count: int

    async def init(self):
        await super().init()
        self.count = 0

    async def handle_call(self, sender: llegos.Actor, message: t.Any):
        self.count += 1
        return "ping"


class Ponger(llegos.Actor):
    count: int

    async def init(self):
        await super().init()
        self.count = 0

    async def handle_call(self, sender: llegos.Actor, message: t.Any):
        self.count += 1
        return "pong"


@pytest.mark.asyncio
async def test_ping_pong():
    pinger = Pinger()
    ponger = Ponger()

    async with create_task_group() as tg:
        tg.start_soon(pinger.loop)
        tg.start_soon(ponger.loop)

        await pinger.started()
        await ponger.started()

        await pinger.call(ponger, "ping")
        assert ponger.count == 1
        assert pinger.count == 0

        await ponger.call(pinger, "pong")
        assert pinger.count == 1

        tg.cancel_scope.cancel()


class FaultyPonger(llegos.Actor):
    count: int

    async def init(self):
        await super().init()
        self.count = 0

    async def handle_call(self, sender: llegos.Actor, message: t.Any):
        self.count += 1
        raise Exception("I'm faulty")


@pytest.mark.asyncio
async def test_supervisor_one_for_one():
    pinger = Pinger()
    ponger = FaultyPonger()

    async with create_task_group() as tg:
        supervisor = llegos.Supervisor(
            children=[pinger, ponger],
            strategy=llegos.Supervisor.Strategy.one_for_one,
        )

        tg.start_soon(supervisor.loop)

        await supervisor.started()  # => supervisor, pinger, and ponger are started

        try:
            await pinger.call(ponger, "ping", 0.1)
            assert False, "ponger should've crashed"
        except Exception:
            ...

        await supervisor.started()
        assert ponger.count == 0

        tg.cancel_scope.cancel()


@pytest.mark.asyncio
@pytest.mark.skip
async def test_worker_pool():
    print()
    llegos.runtime.events.on(
        "after:perform", lambda msg, response: print(msg, response)
    )

    ponger = Ponger()
    pinger_pool = llegos.WorkerPool(children=[Pinger(), Pinger(), Pinger()])

    async with create_task_group() as tg:
        tg.start_soon(ponger.loop)
        tg.start_soon(pinger_pool.loop)

        await ponger.started()
        await pinger_pool.started()

        for _ in range(10):
            print(await ponger.call(pinger_pool, "pong"))

        tg.cancel_scope.cancel()

    assert sum(actor.count for actor in pinger_pool.children) == 10
