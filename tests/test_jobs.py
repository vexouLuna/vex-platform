from __future__ import annotations

import asyncio
import datetime as dt
import time
from typing import Any

import psycopg
import pytest

from vex_platform.actor import Actor
from vex_platform.jobs import (
    InvalidJob,
    JobConflict,
    Registry,
    RunStopped,
    SkipTo,
    StepContext,
    StepDeferred,
    StepError,
    StepRefused,
)
from vex_platform.jobs.runtime import kinds_info

from .conftest import DSN, audit_rows, wait_for

pytestmark = pytest.mark.usefixtures("dsn")

VEX = Actor("user", "1", "vex", "api")


def three_steps(calls: list[str], **kind_options: Any) -> Registry:
    registry = Registry()
    for name in ("a", "b", "c"):

        async def step(ctx: StepContext, name: str = name) -> None:
            calls.append(name)
            ctx.payload[name] = True
            ctx.log.info("did %s", name)

        registry.add_step(name, step)
    registry.kind("abc", ["a", "b", "c"], retry_base_seconds=0, **kind_options)
    return registry


async def test_steps_run_in_order_and_are_recorded(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(three_steps(calls))
    enq = await runtime.enqueue("abc", subject="vod:1", payload={"x": 1}, actor=VEX)
    assert enq.created
    run = await wait_for(runtime, enq.run.id, ["succeeded"])
    assert calls == ["a", "b", "c"]
    assert run.payload == {"x": 1, "a": True, "b": True, "c": True}
    assert run.step is None and run.finished_at is not None and run.actor == VEX

    events = await runtime.events.list(run.id)
    messages = [e["message"] for e in events]
    assert "did a" in messages and "abc finished" in messages
    [row] = await audit_rows("job.enqueue")
    assert (row["actor_login"], row["target"], row["job_run_id"]) == ("vex", f"job:{run.id}", run.id)


async def test_pause_gates_and_resume_once(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(three_steps(calls, pause_before=("b",)))
    run = (await runtime.enqueue("abc")).run
    run = await wait_for(runtime, run.id, ["paused"])
    assert (calls, run.step) == (["a"], "b")

    run = await runtime.resume(run.id, once=True)
    run = await wait_for(runtime, run.id, ["paused"])
    assert (calls, run.step) == (["a", "b"], "c")

    await runtime.resume(run.id)
    await wait_for(runtime, run.id, ["succeeded"])
    assert calls == ["a", "b", "c"]


async def test_gated_first_step_and_run_override(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(three_steps(calls, pause_before=("a",)))
    gated = (await runtime.enqueue("abc")).run
    assert gated.state == "paused" and gated.procrastinate_job_id is None
    free = (await runtime.enqueue("abc", pause_before=[])).run
    await wait_for(runtime, free.id, ["succeeded"])
    with pytest.raises(JobConflict):
        await runtime.resume(free.id)


async def test_kind_gates_change_while_a_run_goes(make_runtime):
    calls: list[str] = []
    gate = asyncio.Event()
    registry = three_steps(calls)

    async def slow_a(ctx: StepContext) -> None:
        calls.append("a")
        await gate.wait()

    registry.steps["a"] = slow_a
    runtime = await make_runtime(registry)
    run = (await runtime.enqueue("abc")).run
    own = (await runtime.enqueue("abc", pause_before=[])).run
    await wait_for(runtime, run.id, ["running"])
    await wait_for(runtime, own.id, ["running"])

    registry.set_pause_before("abc", ["c"])  # after both started
    assert [k["pause_before"] for k in kinds_info(registry)] == [["c"]]
    gate.set()
    run = await wait_for(runtime, run.id, ["paused"])
    assert run.step == "c"
    await wait_for(runtime, own.id, ["succeeded"])  # its own (empty) list wins
    assert (await runtime.enqueue("abc", step="c")).run.state == "paused"
    with pytest.raises(InvalidJob):
        registry.set_pause_before("abc", ["nope"])


async def test_pause_queued_and_running(make_runtime):
    registry = Registry()
    release = asyncio.Event()

    @registry.step()
    async def wait(ctx):
        await release.wait()

    @registry.step()
    async def after(ctx):
        pass

    registry.kind("w", ["wait", "after"])
    runtime = await make_runtime(registry, start=False)
    queued = (await runtime.enqueue("w")).run
    paused = await runtime.pause(queued.id)
    assert paused.state == "paused" and paused.procrastinate_job_id is None

    await runtime.start()
    await runtime.resume(paused.id)
    await wait_for(runtime, paused.id, ["running"])
    running = await runtime.pause(paused.id)
    assert running.state == "running" and running.pause_next
    release.set()
    run = await wait_for(runtime, paused.id, ["paused"])
    assert run.step == "after" and not run.pause_next


async def test_failed_step_retries_then_fails(make_runtime):
    registry = Registry()
    tries: list[int] = []

    @registry.step()
    async def flaky(ctx):
        tries.append(ctx.attempt)
        if ctx.attempt < 2:
            raise StepError("not yet")

    @registry.step()
    async def broken(ctx):
        tries.append(-ctx.attempt)
        raise RuntimeError("boom")

    registry.kind("flaky", ["flaky"], retry_base_seconds=0)
    registry.kind("broken", ["broken"], retry_base_seconds=0, max_attempts=3)
    runtime = await make_runtime(registry)

    ok = (await runtime.enqueue("flaky")).run
    ok = await wait_for(runtime, ok.id, ["succeeded"])
    assert ok.attempts == 1 and ok.last_error is None

    bad = (await runtime.enqueue("broken")).run
    bad = await wait_for(runtime, bad.id, ["failed"])
    assert bad.attempts == 3 and "RuntimeError: boom" in (bad.last_error or "")
    assert [t for t in tries if t < 0] == [-1, -2, -3]

    again = await runtime.retry(bad.id)
    assert again.state == "queued" and again.attempts == 0
    await wait_for(runtime, bad.id, ["failed"])


async def test_retry_backoff_schedules_later(make_runtime):
    registry = Registry()

    @registry.step()
    async def once(ctx):
        raise StepError("later")

    registry.kind("slow_retry", ["once"], retry_base_seconds=3600)
    runtime = await make_runtime(registry)
    run = (await runtime.enqueue("slow_retry")).run
    deadline = time.monotonic() + 10
    while (run := await runtime.get(run.id)).attempts == 0 and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    assert run.state == "queued" and run.last_error == "later"
    assert (run.not_before - run.updated_at).total_seconds() > 3500


async def test_refused_step_fails_at_once(make_runtime):
    registry = Registry()

    @registry.step()
    async def refuse(ctx):
        raise StepRefused("no consent")

    registry.kind("refuse", ["refuse"], retry_base_seconds=0)
    runtime = await make_runtime(registry)
    run = await wait_for(runtime, (await runtime.enqueue("refuse")).run.id, ["failed"])
    assert (run.attempts, run.last_error) == (1, "no consent")


async def test_deferred_step_waits_until_then_with_fresh_attempts(make_runtime):
    registry = Registry()
    until = dt.datetime.now(dt.UTC) + dt.timedelta(hours=6)
    events: list[str] = []

    @registry.step()
    async def limited(ctx):
        ctx.payload["deferrals"] = ctx.payload.get("deferrals", 0) + 1
        raise StepDeferred("daily limit reached", until)

    registry.kind("limited", ["limited"], retry_base_seconds=0, max_attempts=1)
    runtime = await make_runtime(registry)
    runtime.hooks.append(lambda event, run: events.append(event))
    run = (await runtime.enqueue("limited")).run
    deadline = time.monotonic() + 10
    while (run := await runtime.get(run.id)).last_error is None and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    # max_attempts=1 would have failed it: a deferral is not an attempt.
    assert (run.state, run.step, run.attempts, run.last_error) == ("queued", "limited", 0, "daily limit reached")
    assert run.not_before == until
    assert run.payload == {"deferrals": 1}
    assert "deferred" in events


def test_deferred_needs_an_aware_datetime():
    with pytest.raises(ValueError):
        StepDeferred("later", dt.datetime(2026, 1, 1))


async def test_interrupt_cancel(make_runtime):
    registry = Registry()
    started = asyncio.Event()
    interrupted: list[bool] = []

    @registry.step()
    async def slow(ctx):
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            interrupted.append(True)
            raise

    registry.kind("slow", ["slow"])
    runtime = await make_runtime(registry)
    run = (await runtime.enqueue("slow")).run
    await asyncio.wait_for(started.wait(), 10)
    run = await runtime.cancel(run.id, actor=VEX)
    run = await wait_for(runtime, run.id, ["cancelled"])
    assert interrupted == [True] and run.step == "slow"
    assert [r["actor_login"] for r in await audit_rows("job.cancel")] == ["vex"]


async def test_cooperative_cancel(make_runtime):
    registry = Registry()
    started = asyncio.Event()
    seen: list[str] = []

    @registry.step()
    async def careful(ctx):
        started.set()
        try:
            for _ in range(600):
                await ctx.check_stop()
                await asyncio.sleep(0.05)
        except RunStopped:
            seen.append("stopped")
            raise
        except asyncio.CancelledError:
            seen.append("interrupted")
            raise

    registry.kind("careful", ["careful"], cancel_mode="cooperative")
    runtime = await make_runtime(registry)
    run = (await runtime.enqueue("careful")).run
    await asyncio.wait_for(started.wait(), 10)
    await runtime.cancel(run.id)
    await wait_for(runtime, run.id, ["cancelled"])
    assert seen == ["stopped"]


async def test_cancel_queued_and_finished(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(three_steps(calls), start=False)
    run = (await runtime.enqueue("abc")).run
    run = await runtime.cancel(run.id)
    assert run.state == "cancelled"
    with pytest.raises(JobConflict):
        await runtime.cancel(run.id)
    await runtime.start()
    await asyncio.sleep(0.5)
    assert calls == []


async def test_dedupe_keys(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(three_steps(calls), start=False)
    first = await runtime.enqueue("abc", payload={"gaps": [1]}, queued_key="chan:1")
    same = await runtime.enqueue("abc", payload={"gaps": [2]}, queued_key="chan:1")
    assert first.created and not same.created and same.run.id == first.run.id

    merged = await runtime.enqueue(
        "abc",
        payload={"gaps": [2]},
        queued_key="chan:1",
        on_duplicate="merge",
        merge=lambda old, new: {"gaps": sorted(set(old["gaps"]) | set(new["gaps"]))},
    )
    assert merged.run.payload == {"gaps": [1, 2]}
    assert [r["action"] for r in await audit_rows("job.")] == ["job.enqueue", "job.merge"]

    await runtime.enqueue("abc", active_key="vod:9")
    with pytest.raises(JobConflict):
        await runtime.enqueue("abc", active_key="vod:9", on_duplicate="raise")
    other = await runtime.enqueue("abc", active_key="vod:10")
    assert other.created


async def test_lock_keeps_runs_on_one_subject_apart(make_runtime):
    registry = Registry()
    spans: dict[int, list[float]] = {}

    @registry.step()
    async def hold(ctx):
        spans[ctx.run_id] = [time.monotonic()]
        await asyncio.sleep(0.4)
        spans[ctx.run_id].append(time.monotonic())

    registry.kind("hold", ["hold"], lock=lambda run: run.subject)
    runtime = await make_runtime(registry)
    a = (await runtime.enqueue("hold", subject="vod:1")).run
    b = (await runtime.enqueue("hold", subject="vod:1")).run
    c = (await runtime.enqueue("hold", subject="vod:2")).run
    for run in (a, b, c):
        await wait_for(runtime, run.id, ["succeeded"])
    assert spans[b.id][0] >= spans[a.id][1]
    assert spans[c.id][0] < spans[a.id][1]  # another subject is not held back


async def test_concurrency_limit_changes_at_runtime(make_runtime):
    registry = Registry()
    active = [0]
    peak = [0]

    @registry.step()
    async def busy(ctx):
        active[0] += 1
        peak[0] = max(peak[0], active[0])
        await asyncio.sleep(0.3)
        active[0] -= 1

    registry.kind("busy", ["busy"])
    runtime = await make_runtime(registry, concurrency=1)
    runs = [(await runtime.enqueue("busy")).run for _ in range(3)]
    for run in runs:
        await wait_for(runtime, run.id, ["succeeded"])
    assert peak[0] == 1

    assert runtime.set_concurrency(3) == 3
    peak[0] = 0
    runs = [(await runtime.enqueue("busy")).run for _ in range(3)]
    for run in runs:
        await wait_for(runtime, run.id, ["succeeded"])
    assert peak[0] == 3


async def test_shutdown_requeues_and_next_start_resumes(make_runtime):
    registry = Registry()
    started = asyncio.Event()
    calls: list[str] = []

    @registry.step()
    async def first(ctx):
        calls.append("first")

    @registry.step()
    async def slow(ctx):
        calls.append("slow")
        started.set()
        if len(calls) < 4:
            await asyncio.sleep(30)

    registry.kind("two", ["first", "slow"])
    runtime = await make_runtime(registry, shutdown_timeout=0.5)
    run = (await runtime.enqueue("two")).run
    await asyncio.wait_for(started.wait(), 10)
    await runtime.stop()
    run = await runtime.get(run.id)
    assert (run.state, run.step) == ("queued", "slow")

    calls.append("restart")
    await runtime.start()
    await wait_for(runtime, run.id, ["succeeded"])
    assert calls == ["first", "slow", "restart", "slow"]


async def test_reconcile_recovers_a_crashed_run(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(three_steps(calls), start=False)
    run = (await runtime.enqueue("abc")).run
    # As a crash leaves it: the run marked running at step b, its job gone.
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await conn.execute("SET search_path TO jobs")  # procrastinate's triggers use unqualified names
        await conn.execute("DELETE FROM jobs.procrastinate_jobs")
        await conn.execute("UPDATE jobs.job_runs SET state = 'running', step = 'b' WHERE id = %s", (run.id,))
    await runtime.start()  # reconciles first
    await wait_for(runtime, run.id, ["succeeded"])
    assert calls == ["b", "c"]


async def jobs_sql(sql: str, params: Any = None) -> list[tuple[Any, ...]]:
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        await conn.execute("SET search_path TO jobs")  # procrastinate's triggers use unqualified names
        cur = await conn.execute(sql, params)
        return list(await cur.fetchall()) if cur.description else []


def locked(calls: list[str]) -> Registry:
    return three_steps(calls, lock=lambda run: run.subject)


async def test_waiting_run_keeps_a_todo_job(make_runtime):
    """A run waiting for a slot is not fetched, so a crash meanwhile leaves no `doing` job behind."""
    registry = Registry()
    release = asyncio.Event()

    @registry.step()
    async def block(ctx):
        await release.wait()

    registry.kind("block", ["block"])
    runtime = await make_runtime(registry, concurrency=1)
    first = (await runtime.enqueue("block")).run
    await wait_for(runtime, first.id, ["running"])
    second = (await runtime.enqueue("block")).run
    await asyncio.sleep(1.0)  # several poll intervals
    assert await jobs_sql(
        "SELECT status::text FROM procrastinate_jobs WHERE id = %s", (second.procrastinate_job_id,)
    ) == [("todo",)]
    release.set()
    await wait_for(runtime, second.id, ["succeeded"])


async def test_reconcile_frees_a_queued_runs_stalled_job(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(locked(calls), start=False)
    run = (await runtime.enqueue("abc", subject="vod:1")).run
    # As a crash leaves it: the job fetched (doing, its worker gone) but the run still queued.
    await jobs_sql("UPDATE procrastinate_jobs SET status = 'doing', worker_id = NULL")
    await runtime.start()  # reconciles first
    run = await wait_for(runtime, run.id, ["succeeded"])
    assert calls == ["a", "b", "c"]


async def test_reconcile_ends_a_stalled_job_its_run_left_behind(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(locked(calls), start=False)
    run = (await runtime.enqueue("abc", subject="vod:1")).run
    stuck = run.procrastinate_job_id
    # What 0.6.0 left: the stalled job kept its lock through a pause and the resume that followed.
    await jobs_sql("UPDATE procrastinate_jobs SET status = 'doing', worker_id = NULL")
    await jobs_sql("UPDATE job_runs SET state = 'paused', procrastinate_job_id = NULL WHERE id = %s", (run.id,))
    run = await runtime.resume(run.id)
    assert run.procrastinate_job_id != stuck
    await runtime.start()
    await wait_for(runtime, run.id, ["succeeded"])
    assert await jobs_sql("SELECT id FROM procrastinate_jobs WHERE id = %s", (stuck,)) == []


async def test_pause_ends_a_stalled_job_but_not_a_live_ones(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(locked(calls), start=False)
    stalled = (await runtime.enqueue("abc", subject="vod:1")).run
    live = (await runtime.enqueue("abc", subject="vod:2")).run
    worker_id = await runtime.app.job_manager.register_worker()  # a fresh heartbeat
    await jobs_sql(
        "UPDATE procrastinate_jobs SET status = 'doing', worker_id = NULL WHERE id = %s",
        (stalled.procrastinate_job_id,),
    )
    await jobs_sql(
        "UPDATE procrastinate_jobs SET status = 'doing', worker_id = %s WHERE id = %s",
        (worker_id, live.procrastinate_job_id),
    )

    await runtime.pause(stalled.id)
    await runtime.pause(live.id)
    left = await jobs_sql("SELECT id FROM procrastinate_jobs ORDER BY id")
    assert left == [(live.procrastinate_job_id,)]  # the live worker's job ends by itself

    await runtime.resume(stalled.id)
    await runtime.start()
    await wait_for(runtime, stalled.id, ["succeeded"])


async def test_enqueue_joins_the_callers_transaction(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(three_steps(calls), start=False)
    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        await conn.execute("SET search_path TO public")
        run = (await runtime.enqueue("abc", conn=conn)).run
        cur = await conn.execute("SHOW search_path")
        assert await cur.fetchone() == ("public",)  # put back after the block
        await conn.rollback()
    with pytest.raises(Exception, match="no job run"):
        await runtime.get(run.id)
    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        cur = await conn.execute("SELECT count(*) FROM jobs.procrastinate_jobs")
        assert await cur.fetchone() == (0,)
    assert await audit_rows() == []


async def test_find_and_update(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(three_steps(calls), start=False)
    run = (await runtime.enqueue("abc", subject="vod:5", payload={"stream_id": "s1"})).run
    assert [r.id for r in await runtime.find("abc", payload_contains={"stream_id": "s1"})] == [run.id]
    assert await runtime.find("abc", subject="vod:6") == []

    run = await runtime.update(run.id, pause_before=["c"], pause_next=True, actor=VEX)
    assert (run.pause_before, run.pause_next) == (["c"], True)
    run = await runtime.update(run.id, pause_before=None)
    assert run.pause_before is None
    with pytest.raises(Exception, match="has no step"):
        await runtime.update(run.id, pause_before=["zzz"])


async def test_step_context_extras(make_runtime):
    registry = Registry()

    class AppContext:
        def __init__(self, ctx: StepContext) -> None:
            self.ctx = ctx

    @registry.step()
    async def work(app: AppContext):
        ctx = app.ctx
        ctx.subject = "vod:77"
        ctx.progress(1, 4, "parts")
        await ctx.save()
        await ctx.audit("vod.archive", target="vod:77")

    registry.kind("ctx", ["work"])
    runtime = (
        await make_runtime(registry, context_factory=AppContext, actor=None)
        if False
        else await make_runtime(registry, context_factory=AppContext)
    )
    run = (await runtime.enqueue("ctx", actor=VEX, scope="456")).run
    run = await wait_for(runtime, run.id, ["succeeded"])
    assert run.subject == "vod:77" and run.scope == "456"
    events = await runtime.events.list(run.id)
    assert any(e["progress"] == {"done": 1, "total": 4, "unit": "parts"} for e in events)
    [row] = await audit_rows("vod.")
    assert (row["actor_login"], row["job_run_id"], row["scope"]) == ("vex", run.id, "456")


async def test_every_audit_row_of_a_run_carries_its_scope(make_runtime):
    runtime = await make_runtime(three_steps([]))
    later = dt.datetime.now(dt.UTC) + dt.timedelta(hours=1)
    run = (await runtime.enqueue("abc", payload={}, actor=VEX, scope="456", not_before=later)).run
    assert run.scope == "456"
    await runtime.pause(run.id, actor=VEX)
    await runtime.resume(run.id, actor=VEX)
    await runtime.cancel(run.id, actor=VEX, wait=0)
    rows = await audit_rows("job.")
    assert [(r["action"], r["scope"]) for r in rows] == [
        ("job.enqueue", "456"),
        ("job.pause", "456"),
        ("job.resume", "456"),
        ("job.cancel", "456"),
    ]


async def test_step_enqueue_links_children_to_their_parent(make_runtime):
    registry = Registry()

    @registry.step()
    async def fan_out(ctx: StepContext) -> None:
        for n in (1, 2):
            await ctx.enqueue("child", subject=f"vod:{n}", payload={"n": n})

    @registry.step()
    async def leaf(ctx: StepContext) -> None:
        if ctx.payload["n"] == 1:
            await ctx.enqueue("leafless", subject="vod:1")

    @registry.step()
    async def noop(ctx: StepContext) -> None:
        pass

    registry.kind("parent", ["fan_out"])
    registry.kind("child", ["leaf"])
    registry.kind("leafless", ["noop"])
    runtime = await make_runtime(registry)
    root = (await runtime.enqueue("parent", subject="channel:1", actor=VEX, scope="chan")).run
    await wait_for(runtime, root.id, ["succeeded"])

    children = await runtime.list(parent_id=root.id)
    assert sorted(c.subject for c in children) == ["vod:1", "vod:2"]
    for child in children:
        assert child.parent_id == root.id and child.scope == "chan"
        assert child.actor == Actor("job", str(root.id), "parent", "job")
        await wait_for(runtime, child.id, ["succeeded"])
    [grandchild] = await runtime.list(kind="leafless")
    assert grandchild.parent_id in {c.id for c in children}

    for start in (root.id, grandchild.id):
        root_id, runs, truncated = await runtime.related(start)
        assert (root_id, truncated) == (root.id, False)
        assert [r.id for r in runs] == sorted([root.id, grandchild.id, *(c.id for c in children)])
    _, runs, truncated = await runtime.related(root.id, limit=2)
    assert len(runs) == 2 and truncated

    alone = (await runtime.enqueue("leafless", subject="vod:9")).run
    root_id, runs, _ = await runtime.related(alone.id)
    assert root_id == alone.id and [r.id for r in runs] == [alone.id] and runs[0].parent_id is None

    # a "job" actor alone sets the parent too
    manual = (await runtime.enqueue("leafless", actor=Actor("job", str(root.id), "parent", "job"))).run
    assert manual.parent_id == root.id
    rows = [r for r in await audit_rows("job.enqueue") if r["after"].get("parent_id") == root.id]
    assert len(rows) == 3


async def test_scheduled_run_waits_without_holding_its_lock(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(locked(calls))
    later = dt.datetime.now(dt.UTC) + dt.timedelta(days=3)
    scheduled = (await runtime.enqueue("abc", subject="vod:1", not_before=later)).run
    assert scheduled.state == "queued" and scheduled.procrastinate_job_id is None
    now = (await runtime.enqueue("abc", subject="vod:1")).run
    await wait_for(runtime, now.id, ["succeeded"])  # not held back by the run three days out
    assert await runtime.reconcile() == 0
    assert (await runtime.get(scheduled.id)).procrastinate_job_id is None

    await jobs_sql("UPDATE job_runs SET not_before = now() WHERE id = %s", (scheduled.id,))
    assert await runtime.reconcile() == 1  # its time came: it gets its job and lock
    await wait_for(runtime, scheduled.id, ["succeeded"])
    assert calls == ["a", "b", "c"] * 2


async def test_retry_a_succeeded_run_later_from_a_step_with_new_gates(make_runtime):
    calls: list[str] = []
    runtime = await make_runtime(three_steps(calls))
    run = await wait_for(runtime, (await runtime.enqueue("abc")).run.id, ["succeeded"])

    at = dt.datetime.now(dt.UTC) + dt.timedelta(hours=2)
    again = await runtime.retry(run.id, step="b", at=at, actor=VEX)
    assert (again.state, again.step, again.not_before, again.procrastinate_job_id) == ("queued", "b", at, None)
    [row] = await audit_rows("job.retry")
    assert row["before"] == {"state": "succeeded"} and row["after"]["not_before"] == at.isoformat()
    await jobs_sql("UPDATE job_runs SET not_before = now() WHERE id = %s", (run.id,))
    await runtime.reconcile()
    await wait_for(runtime, run.id, ["succeeded"])
    assert calls == ["a", "b", "c", "b", "c"]

    # From the first step, stopping before b this time; then back to the kind's gates.
    gated = await runtime.retry(run.id, pause_before=["b"])
    gated = await wait_for(runtime, gated.id, ["paused"])
    assert (gated.step, gated.pause_before) == ("b", ["b"])
    await runtime.cancel(run.id)
    assert (await runtime.retry(run.id, step="a", pause_before=["a"])).state == "paused"  # gated where it starts
    await runtime.cancel(run.id)
    assert (await runtime.retry(run.id, pause_before=None)).pause_before is None
    with pytest.raises(JobConflict):
        await runtime.retry(run.id)  # still active
    with pytest.raises(InvalidJob):
        await runtime.retry(run.id, at=dt.datetime(2030, 1, 1))


async def test_skip_to_a_later_step_or_the_end(make_runtime):
    registry = Registry()
    calls: list[str] = []

    @registry.step()
    async def split(ctx):
        calls.append("split")
        if ctx.payload.get("to") != "split":
            raise SkipTo("nothing to upload", ctx.payload.get("to"))

    for name in ("upload", "cleanup"):

        async def step(ctx: StepContext, name: str = name) -> None:
            calls.append(name)

        registry.add_step(name, step)
    registry.kind("vod", ["split", "upload", "cleanup"], retry_base_seconds=0)
    runtime = await make_runtime(registry)

    run = await wait_for(runtime, (await runtime.enqueue("vod", payload={"to": "cleanup"})).run.id, ["succeeded"])
    assert calls == ["split", "cleanup"] and run.attempts == 0
    calls.clear()
    run = await wait_for(runtime, (await runtime.enqueue("vod", payload={"to": None})).run.id, ["succeeded"])
    assert calls == ["split"] and run.last_error is None
    assert "nothing to upload; nothing left to do" in [e["message"] for e in await runtime.events.list(run.id)]
    run = await wait_for(runtime, (await runtime.enqueue("vod", payload={"to": "split"})).run.id, ["succeeded"])
    bad = await wait_for(runtime, (await runtime.enqueue("vod", payload={"to": "nope"})).run.id, ["failed"])
    assert "not a later step" in (bad.last_error or "") and bad.attempts == 1
