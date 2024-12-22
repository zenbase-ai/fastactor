import anyio
import pytest
import asyncio

from fastactor.otp import (
    Call,
    Cast,
    Down,
    Exit,
    Runtime,
    GenServer,
    Supervisor,
    RuntimeSupervisor,
    ActorFailed,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
async def runtime():
    """
    This fixture starts a fresh Runtime for each test, and ensures cleanup.
    """
    async with Runtime() as rt:
        yield rt


@pytest.fixture
async def supervisor(runtime: Runtime):
    assert runtime.supervisor
    return runtime.supervisor


async def test_process_lifecycle(runtime: Runtime):
    """
    G: a Runtime is active, we spawn a GenServer G1.
    W: G1 calls exit("normal") or stops normally.
    T: runtime's dictionary no longer has G1, no abnormal signals to linked procs.
    """

    # Given
    G1 = await GenServer.start()
    assert G1.has_started()

    # Then
    assert G1.id in runtime.processes

    # When
    await G1.stop("normal")

    # Then
    assert G1.has_stopped()
    # We assume no abnormal exit occurred => no ActorCrashed
    assert not isinstance(G1._crash_exc, Exception)

    assert G1.id not in runtime.processes


async def test_named_process_lookup(runtime: Runtime):
    """
    G: A Runtime with a GenServer G2, and we call runtime.register_name("foo_server", G2).
    W: we do pid = runtime.where_is("foo_server").
    T: pid == G2.
    W: we kill G2 => the runtime's processes no longer has G2.
    T: runtime.where_is("foo_server") returns None.
    """

    # Given
    G2 = await GenServer.start()
    runtime.register_name("foo_server", G2)

    # When
    proc = runtime.where_is("foo_server")

    # Then
    assert proc is not None
    assert proc == G2

    # When we kill G2
    await G2.kill()
    await G2.stopped()

    assert runtime.where_is("foo_server") is None
    assert G2.id not in runtime.processes


class CallReplyServer(GenServer):
    async def handle_call(self, call: Call):
        if call.message == "ping":
            return "pong"
        else:
            raise ValueError(call.message)


async def test_genserver_call_reply(runtime: Runtime):
    """
    G: G3 that returns "pong" if request=="ping", else raises => abnormal exit
    W: call(G3, "ping") => "pong"
    W: call(G3, "bad_input") => triggers an exception => G3 exits
    T: G3 is removed from processes, call raises or returns error
    """
    G3 = await CallReplyServer.start()

    # 1) "ping"
    reply = await G3.call("ping")
    assert reply == "pong"

    # 2) "bad_input"
    with pytest.raises(ValueError, match="bad_input"):
        await G3.call("bad_input")

    # Then G3 should be exited abnormally:
    await asyncio.sleep(0.1)  # give time to crash
    assert G3.has_stopped()
    assert isinstance(G3._crash_exc, ValueError)


class MathServer(GenServer):
    count: int

    async def init(self, count: int = 0):
        self.count = count

    async def handle_cast(self, msg: Cast):
        match msg.message:
            case ("add", n):
                self.count += n
            case ("sub", n):
                self.count -= n
            case ("mul", n):
                self.count *= n
            case ("div", n):
                self.count /= n
            case _:
                raise ValueError(f"unknown message: {msg.message}")


async def test_genserver_cast(runtime: Runtime):
    """
    G: G4 has handle_cast => increments self.state["count"]
    W: cast("increment") multiple times
    T: calls return immediately, state increments each time
    """

    G4 = await MathServer.start(count=42)

    # cast
    G4.cast(("div", 2))
    G4.cast(("add", 4))
    G4.cast(("div", 5))
    G4.cast(("mul", 2))

    # Let them process
    await asyncio.sleep(0.2)

    # Then
    assert G4.count == 10

    # Stop
    await G4.stop()
    assert G4.has_stopped()


class CrashingServer(GenServer):
    async def handle_call(self, msg: Call):
        if msg.message == "crash":
            raise RuntimeError("Crash!")


async def test_supervisor_one_for_one(runtime: Runtime, supervisor: Supervisor):
    """
    G: Sup1 with child transient, if child crashes abnormally => restart, if normal => no restart
    W: child crashes => new child is started
    W: child later exits normal => no restart
    T: verifies the child is replaced only on abnormal
    """
    # Add a single child, transient
    cA = await supervisor.start_child(
        "childA", supervisor.child_spec(CrashingServer, restart="transient")
    )
    # We have a single child
    assert len(supervisor.which_children()) == 1
    # abnormal crash
    with pytest.raises(RuntimeError, match="Crash!"):
        await cA.call("crash")

    await anyio.sleep(0.2)
    # Should have restarted child
    cA2 = supervisor.children["childA"][0]
    assert id(cA2) != id(cA), "should have restarted the child"

    # normal exit => no restart
    await cA2.stop("normal")
    await anyio.sleep(0.2)
    assert "childA" not in supervisor.children, "should have removed the child"
    # The child is not restarted if normal exit => ephemeral


class BoomServer(GenServer):
    async def handle_call(self, msg: Call):
        if msg.message == "boom":
            raise RuntimeError("Boom!")
        return "ok"


async def xtest_supervisor_one_for_all(runtime: Runtime):
    """
    G: Sup2 with strategy=one_for_all, children [C1, C2, C3]
    W: C2 crashes => all are killed => all are restarted
    T: each child has new IDs
    """

    sup2_spec = runtime.supervisor.child_spec(
        Supervisor, kwargs={"strategy": "one_for_all"}
    )
    sup2 = await runtime.supervisor.start_child("sup2", sup2_spec)

    # Add 3 children
    for cid in ["C1", "C2", "C3"]:
        c_spec = sup2.child_spec(BoomServer, restart="transient")
        await sup2.start_child(cid, c_spec)

    c1_old = sup2.children["C1"][0]
    c2_old = sup2.children["C2"][0]
    c3_old = sup2.children["C3"][0]

    # Crash C2
    with pytest.raises(RuntimeError, match="Boom!"):
        await c2_old.call("boom")
    await asyncio.sleep(0.3)

    # All replaced
    c1_new = sup2.children["C1"][0]
    c2_new = sup2.children["C2"][0]
    c3_new = sup2.children["C3"][0]

    assert id(c1_new) != id(c1_old)
    assert id(c2_new) != id(c2_old)
    assert id(c3_new) != id(c3_old)


class LinkServer(GenServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exits_received = []

    async def handle_exit(self, msg: Exit):
        self.exits_received.append((msg.sender, msg.reason))


async def test_linking_trap_exits(runtime: Runtime):
    """
    G: Two GenServers A and B. A.link(B), A.trap_exits=True.
    W: B.exit("error") abnormally
    T: A not killed, A.handle_info receives "EXIT(B, error)" or handle_exit is called
    If A.trap_exits=False => A would crash too
    """

    A = await LinkServer.start(trap_exits=True)
    B = await LinkServer.start(trap_exits=False)

    A.link(B)

    # B calls stop with abnormal reason
    await B.stop("error")
    await B.stopped()

    # A should remain alive
    assert not A.has_stopped()

    # The abnormal exit is passed to A's handle_exit
    assert len(A.exits_received) == 1
    assert A.exits_received[0][1] == "error"

    # Now if we had not set trap_exits=True in A => it would have died as well
    # We'll stop A now
    await A.stop("normal")


class MonitorServer(GenServer):
    async def init(self, *args, **kwargs):
        self.down_msgs = []

    async def handle_info(self, message):
        if isinstance(message, Down):
            self.down_msgs.append(message)


async def test_monitoring(runtime: Runtime):
    """
    G: M monitors T (T.monitor(M)).
    W: T exits "normal".
    T: M.handle_info("DOWN", T, "normal") => M remains alive
    """

    M = await MonitorServer.start()
    T_ = await GenServer.start()

    # M monitors T
    M.monitor(T_)

    # T stops normally
    await T_.stop("normal")
    await T_.stopped()

    # M remains alive
    assert not M.has_stopped()

    # M should eventually get a Down message
    await asyncio.sleep(0.2)
    assert len(M.down_msgs) == 1
    down_msg = M.down_msgs[0]
    assert down_msg[0] == "DOWN"
    assert down_msg[1] == T_
    assert down_msg[2] == "normal"

    # stop M
    await M.stop("normal")


async def test_chain_crash_via_linking(runtime: Runtime):
    """
    G: X, Y, Z. X.link(Y), Y.link(Z), none trap_exits
    W: Z crashes with reason "fatal"
    T: Y sees crash => Y also crashes => X sees crash => X also crashes
    => all removed
    """

    X = await GenServer.start(trap_exits=False)
    Y = await GenServer.start(trap_exits=False)
    Z = await GenServer.start(trap_exits=False)

    X.link(Y)
    Y.link(Z)

    await Z.stop("fatal")
    await Z.stopped()

    # chain reaction => Y, then X => all gone
    await asyncio.sleep(0.3)

    assert X.has_stopped()
    assert Y.has_stopped()
    # all have abnormal reason
    assert X._crash_exc == "fatal"
    assert Y._crash_exc == "fatal"
    assert Z._crash_exc == "fatal"


class Counter(GenServer):
    async def init(self, *args, **kwargs):
        self.state = 0

    async def handle_call(self, msg: Call):
        # we do some async waiting to test concurrency
        await asyncio.sleep(0.05)
        if msg.message == "inc":
            self.state += 1
            return self.state


async def test_parallel_calls_queue_behavior(runtime: Runtime):
    """
    G: A GenServer P increments an internal counter for each call
    W: multiple calls concurrently
    T: processed FIFO, final count is sum of calls
    """

    P = await Counter.start()

    calls = [asyncio.create_task(P.call("inc")) for _ in range(5)]
    results = await asyncio.gather(*calls)
    assert results == [1, 2, 3, 4, 5]
    assert P.state == 5
    await P.stop("normal")


class AlwaysCrashes(GenServer):
    async def init(self, *args, **kwargs):
        raise RuntimeError("I always crash")


async def test_supervisor_crash(runtime: Runtime):
    """
    G: Sup3 with max_restarts=2, child that always crashes => 3 crashes => sup fails
    W: child keeps failing, eventually sup => ActorFailed
    T: sup removed, any watchers notified
    """

    sup3_spec = runtime.supervisor.child_spec(
        Supervisor, kwargs={"max_restarts": 2, "max_seconds": 5}
    )
    sup3 = await runtime.supervisor.start_child("sup3", sup3_spec)

    # add child that always crashes on startup
    with pytest.raises(ActorFailed, match="Max restart intensity reached"):
        await sup3.start_child(
            "bad_child", sup3.child_spec(AlwaysCrashes, restart="transient")
        )
        await sup3.stopped()

    # sup3 should be removed from parent's children
    assert "sup3" not in runtime.supervisor.children


class Mon(GenServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.downs = []

    async def handle_info(self, message):
        if isinstance(message, Down):
            self.downs.append(message)


async def test_multiple_monitors_links(runtime: Runtime):
    """
    G: process Z monitored by [M1, M2, M3] and linked to L1
    W: Z.exit("boom")
    T: each M? sees ("DOWN", Z, "boom"), L1 goes down if trap_exits=False
    """

    M1 = await Mon.start()
    M2 = await Mon.start()
    M3 = await Mon.start()
    L1 = await GenServer.start(trap_exits=False)
    Z = await GenServer.start()

    # Monitor Z
    M1.monitor(Z)
    M2.monitor(Z)
    M3.monitor(Z)
    # Link L1 with Z
    L1.link(Z)

    # Z => "boom"
    await Z.stop("boom")
    await Z.stopped()
    # Wait for chain reaction
    await asyncio.sleep(0.2)

    # All M? got "DOWN"
    for M in (M1, M2, M3):
        assert len(M.downs) == 1
        assert M.downs[0][0] == "DOWN"
        assert M.downs[0][1] == Z
        assert M.downs[0][2] == "boom"

    # L1 => crashes if not trap_exits => "boom"
    await L1.stopped()
    assert L1._crash_exc == "boom"

    # M1, M2, M3 remain alive
    for M in (M1, M2, M3):
        assert not M.has_stopped()

    # Clean up
    for M in (M1, M2, M3):
        await M.stop("normal")


class RootServer(Supervisor):
    pass


async def test_named_supervisor_tree(runtime: Runtime, supervisor: Supervisor):
    """
    G: spawn RootSup, register as "root"
    W: spawn children [SupA, SupB] in RootSup, each with own GenServer kids
    T: we can do root_id = where_is("root"), from there .which_children()
    """

    # We'll define a minimal root

    # spawn root
    root_spec = supervisor.child_spec(RootServer, args=(), kwargs={})
    root = await supervisor.start_child("RootSup", root_spec)

    runtime.register_name("root", root)
    assert runtime.where_is("root") == root

    # spawn sub supervisors
    subA_spec = root.child_spec(Supervisor, restart="permanent")
    subB_spec = root.child_spec(Supervisor, restart="permanent")

    supA = await root.start_child("SupA", subA_spec)
    supB = await root.start_child("SupB", subB_spec)

    # e.g. 2 child servers each
    server_spec = supA.child_spec(GenServer, restart="transient")
    await supA.start_child("childA1", server_spec)
    await supA.start_child("childA2", server_spec)

    server_spec2 = supB.child_spec(GenServer, restart="transient")
    await supB.start_child("childB1", server_spec2)
    await supB.start_child("childB2", server_spec2)

    # we can check root's children
    rc = root.which_children()
    assert len(rc) == 2  # SupA, SupB

    # subA has 2 children
    ac = supA.which_children()
    assert len(ac) == 2

    # subB has 2 children
    bc = supB.which_children()
    assert len(bc) == 2

    await root.stop("normal")

    assert root.has_stopped()
