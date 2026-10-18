"""The job runtime: ``job_runs`` is the record of every run, procrastinate only moves them.

Each queued run has one procrastinate job, ``vex.run(run_id)``, that runs the run's steps from its
checkpoint (``job_runs.step``) until the run finishes, fails, or reaches a pause gate. Retries,
resume and recovery defer a fresh procrastinate job for the same run; a job that no longer matches
its run (``job_runs.procrastinate_job_id``) does nothing. docs/conventions.md, "Jobs", has the
states and the rules.

    runtime = JobRuntime(registry, dsn, audit_table="public.audit_log")
    await runtime.open()
    await runtime.start()            # recover, then the in-process worker
    run = (await runtime.enqueue("archive", subject="vod:1", actor=actor)).run
    ...
    await runtime.close()            # stop the worker (runs re-queue at their step), close the pool
"""

from __future__ import annotations

import asyncio
import builtins
import datetime as dt
import traceback
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from contextlib import asynccontextmanager, suppress
from typing import Any, Literal, NamedTuple

import procrastinate
import procrastinate.worker
import structlog
from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from ..actor import SYSTEM, Actor
from ..audit import psycopg as audit_pg
from ..audit.model import AuditEntry
from .context import StepContext
from .errors import InvalidJob, JobConflict, JobNotFound, RunStopped, SkipTo, StepDeferred, StepError, StepRefused
from .events import RunEvents
from .registry import JobKind, Registry
from .run import ACTIVE, FINISHED, SELECT, STATES, JobRun

log = structlog.get_logger(__name__)

TASK_NAME = "vex.run"
# A ``doing`` job whose worker sent no heartbeat for this long, or has no worker row, is stalled
# (the default of procrastinate's get_stalled_jobs).
STALLED_AFTER = 30.0
OnDuplicate = Literal["return", "merge", "raise"]
Hook = Callable[[str, JobRun], Any]


class Enqueued(NamedTuple):
    run: JobRun
    created: bool  # False: an existing run with the same queued_key/active_key was returned


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class Limiter:
    """A concurrency limit that can change while runs hold slots (lowering it lets running runs
    finish; new ones wait until the count is under the new limit)."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.active = 0
        self._changed = asyncio.Event()

    async def acquire(self) -> None:
        while self.active >= self.limit:
            self._changed.clear()
            await self._changed.wait()
        self.active += 1

    def release(self) -> None:
        self.active -= 1
        self._changed.set()

    def set_limit(self, limit: int) -> None:
        self.limit = limit
        self._changed.set()


class _Slots:
    """Stands in for the procrastinate worker's semaphore, so the worker fetches a job (and marks it
    ``doing``) only once the limiter has a free slot. A run waiting for a slot keeps a ``todo`` job:
    nothing is left ``doing`` if the process dies while it waits, and pause or cancel can drop it."""

    def __init__(self, limiter: Limiter) -> None:
        self.limiter = limiter

    async def acquire(self) -> bool:
        await self.limiter.acquire()
        return True

    def release(self) -> None:
        self.limiter.release()


class _Worker(procrastinate.worker.Worker):
    """procrastinate's worker, fetching through the runtime's slots instead of its own semaphore.
    ``_job_semaphore`` is private to procrastinate; the exact pin in pyproject.toml keeps it known,
    and test_waiting_run_keeps_a_todo_job fails if an upgrade changes it."""

    slots: _Slots

    @property
    def _job_semaphore(self) -> Any:
        return self.slots

    @_job_semaphore.setter
    def _job_semaphore(self, value: Any) -> None:
        pass  # procrastinate's own asyncio.Semaphore, created in __init__ and _run_loop: unused


class JobRuntime:
    """
    ``conninfo``: the database; the runtime keeps its own psycopg pool with ``search_path`` set to
    ``schema``, where ``migrations.jobs_sql`` created the tables.
    ``audit_table``: schema-qualified (``public.audit_log``), so it is reachable from that pool.
    When set, every action on a run (``job.enqueue``, ``job.cancel`` ...) writes an audit row in the
    same transaction, and ``ctx.audit`` works in steps.
    ``concurrency``: runs at once, changeable with ``set_concurrency`` up to ``max_concurrency``.
    ``context_factory(ctx)``: what step functions receive instead of the ``StepContext``, for an
    application's own context type.
    """

    def __init__(
        self,
        registry: Registry,
        conninfo: str,
        *,
        schema: str = "jobs",
        audit_table: str | None = None,
        concurrency: int = 3,
        max_concurrency: int = 16,
        max_attempts: int = 3,
        context_factory: Callable[[StepContext], Any] | None = None,
        pool_min_size: int = 1,
        pool_max_size: int = 8,
        worker_name: str = "vex",
        shutdown_timeout: float = 10.0,
        poll_interval: float = 5.0,
        reconcile_interval: float = 60.0,
    ) -> None:
        registry.validate()
        self.registry = registry
        self.schema = schema
        self.audit_table = audit_table
        self.max_concurrency = max_concurrency
        self.max_attempts = max_attempts
        self.context_factory = context_factory
        self.worker_name = worker_name
        self.shutdown_timeout = shutdown_timeout
        self.poll_interval = poll_interval
        self.reconcile_interval = reconcile_interval
        self.limiter = Limiter(min(concurrency, max_concurrency))
        self.hooks: list[Hook] = []
        self.stopping = False

        self.pool = AsyncConnectionPool(
            conninfo,
            min_size=pool_min_size,
            max_size=pool_max_size,
            kwargs={"options": f"-c search_path={schema}"},
            open=False,
            check=AsyncConnectionPool.check_connection,
        )
        self.events = RunEvents(self.pool)
        self.app = procrastinate.App(connector=procrastinate.PsycopgConnector())

        async def vex_run(context: procrastinate.JobContext, run_id: int) -> None:
            await self._execute(context, run_id)

        self.task = self.app.task(name=TASK_NAME, pass_context=True)(vex_run)
        self._tasks: dict[int, asyncio.Task[Any]] = {}  # runs executing in this process
        self._cancelling: set[int] = set()  # of those, the ones asked to stop
        self._background: list[asyncio.Task[Any]] = []
        self._worker: asyncio.Task[Any] | None = None

    # ---- lifecycle ---------------------------------------------------------------------------

    async def open(self) -> None:
        await self.pool.open(wait=True)
        await self.app.open_async(self.pool)

    async def start(self) -> None:
        """Recover runs a previous process left behind, then run the worker in this event loop."""
        self.stopping = False
        await self.reconcile()
        self._background = [
            asyncio.create_task(self.events.run_forever(), name="vex-jobs-events"),
            asyncio.create_task(self._reconcile_forever(), name="vex-jobs-reconcile"),
        ]
        worker = _Worker(
            app=self.app,
            name=self.worker_name,
            concurrency=self.max_concurrency,
            install_signal_handlers=False,
            shutdown_graceful_timeout=self.shutdown_timeout,
            fetch_job_polling_interval=self.poll_interval,
            abort_job_polling_interval=1.0,
            listen_notify=True,
            delete_jobs="always",
        )
        worker.slots = _Slots(self.limiter)
        self._worker = asyncio.create_task(
            worker.run(),  # type: ignore[no-untyped-call]  # procrastinate leaves it unannotated
            name="vex-jobs-worker",
        )

    async def stop(self) -> None:
        """Stop taking runs. Cooperative steps see ``should_stop()``; after ``shutdown_timeout`` the
        rest are interrupted. Either way an unfinished run goes back to ``queued`` at its step."""
        self.stopping = True
        if self._worker is not None:
            self._worker.cancel()
            with suppress(asyncio.CancelledError):
                await self._worker
            self._worker = None
        for task in self._background:
            task.cancel()
        await asyncio.gather(*self._background, return_exceptions=True)
        self._background = []

    async def close(self) -> None:
        await self.stop()
        await self.events.flush()
        await self.app.close_async()
        await self.pool.close()

    @property
    def running(self) -> bool:
        return self._worker is not None and not self._worker.done()

    @property
    def concurrency(self) -> int:
        return self.limiter.limit

    def set_concurrency(self, n: int) -> int:
        """Change how many runs execute at once, now; returns the limit applied (1..max_concurrency)."""
        n = max(1, min(n, self.max_concurrency))
        self.limiter.set_limit(n)
        return n

    # ---- connections ---------------------------------------------------------------------------

    @asynccontextmanager
    async def transaction(self, conn: AsyncConnection[Any] | None = None) -> AsyncIterator[AsyncConnection[Any]]:
        """A transaction on the runtime's pool, or on ``conn`` (a savepoint if it is already in one)
        with the jobs schema put first on its search_path until the block ends."""
        if conn is None:
            async with self.pool.connection() as own, own.transaction():
                yield own
            return
        async with conn.transaction():
            cur = await conn.execute("SELECT current_setting('search_path')")
            row = await cur.fetchone()
            assert row is not None  # current_setting() always returns a row
            previous = row[0] if not isinstance(row, dict) else row["current_setting"]
            await conn.execute("SELECT set_config('search_path', %s, true)", (f"{self.schema}, {previous}",))
            yield conn
            # Only on success: a rolled-back savepoint takes the setting back with it.
            await conn.execute("SELECT set_config('search_path', %s, true)", (previous,))

    async def _one(self, conn: AsyncConnection[Any], sql: str, params: Any = None) -> dict[str, Any] | None:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return await cur.fetchone()

    async def _all(self, conn: AsyncConnection[Any], sql: str, params: Any = None) -> list[dict[str, Any]]:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return list(await cur.fetchall())

    async def _lock_run(self, conn: AsyncConnection[Any], run_id: int) -> JobRun:
        row = await self._one(conn, f"SELECT {SELECT} FROM job_runs WHERE id = %s FOR UPDATE", (run_id,))
        if row is None:
            raise JobNotFound(run_id)
        return JobRun.from_row(row)

    async def _update(self, conn: AsyncConnection[Any], run_id: int, **values: Any) -> JobRun:
        sets = ", ".join(f"{k} = %({k})s" for k in values)
        params = {k: Jsonb(v) if k == "payload" else v for k, v in values.items()}
        row = await self._one(
            conn, f"UPDATE job_runs SET {sets} WHERE id = %(_id)s RETURNING {SELECT}", {**params, "_id": run_id}
        )
        if row is None:
            raise JobNotFound(run_id)
        return JobRun.from_row(row)

    def _scheduled(self, run: JobRun) -> bool:
        """Whether ``run`` starts later than the next ``reconcile()``: it waits with no procrastinate
        job, so it holds no lock (a todo job would hold back every later run with the same lock)."""
        return run.not_before is not None and run.not_before > _now() + dt.timedelta(seconds=self.reconcile_interval)

    async def _defer(self, conn: AsyncConnection[Any], run: JobRun, *, hold: bool = False) -> JobRun:
        """Queue a procrastinate job for ``run`` on ``conn`` (so it commits with the run's change).
        A run scheduled past the next reconcile gets its job from that reconcile instead, unless it
        ``hold``s its lock while it waits (a retry backoff, a ``StepDeferred``)."""
        if not hold and self._scheduled(run):
            return (
                run if run.procrastinate_job_id is None else await self._update(conn, run.id, procrastinate_job_id=None)
            )
        kind = self.registry.get(run.kind)
        options: dict[str, Any] = {"queue": kind.queue, "priority": kind.priority, "connection": conn}
        lock = kind.lock(run) if kind.lock else None
        if lock:
            options["lock"] = lock
        if run.not_before is not None and run.not_before > _now():
            options["schedule_at"] = run.not_before
        job_id = await self.task.configure(**options).defer_async(run_id=run.id)
        return await self._update(conn, run.id, procrastinate_job_id=job_id)

    async def _drop_job(self, conn: AsyncConnection[Any], run: JobRun) -> None:
        """Cancel the run's procrastinate job if it has not started, or end it if the worker that took
        it is gone (its lock would hold later jobs back for good). A live worker's job ends by itself."""
        if run.procrastinate_job_id is not None and not await self.app.job_manager.cancel_job_by_id_async(
            run.procrastinate_job_id, delete_job=True, connection=conn
        ):
            await self._finish_stalled(conn, run.procrastinate_job_id)

    async def _finish_stalled(self, conn: AsyncConnection[Any], job_id: int) -> bool:
        """Fail and delete job ``job_id`` if it is ``doing`` and stalled (no worker row, or no heartbeat
        for STALLED_AFTER seconds, procrastinate's own definition); returns whether it was. What
        ``finish_job_by_id_async(job_id, FAILED, delete_job=True)`` does, but on ``conn`` so it commits
        with the run's change, and without an error when another process got there first."""
        row = await self._one(
            conn,
            "DELETE FROM procrastinate_jobs j WHERE j.id = %s AND j.status = 'doing' AND NOT EXISTS ("
            " SELECT 1 FROM procrastinate_workers w WHERE w.id = j.worker_id"
            " AND w.last_heartbeat >= now() - make_interval(secs => %s)) RETURNING j.id",
            (job_id, STALLED_AFTER),
        )
        return row is not None

    async def _audit(
        self,
        conn: AsyncConnection[Any],
        action: str,
        run: JobRun,
        actor: Actor,
        request_id: str | None,
        scope: str | None = None,
        before: Any = None,
        after: Any = None,
        detail: Any = None,
    ) -> None:
        if self.audit_table is None:
            return
        await audit_pg.record(
            conn,
            AuditEntry(
                action,
                actor=actor,
                target=f"job:{run.id}",
                scope=run.scope if scope is None else scope,
                before=before,
                after=after,
                detail=detail,
                request_id=request_id,
                job_run_id=run.id,
            ),
            table=self.audit_table,
        )

    def _hook(self, event: str, run: JobRun) -> None:
        for hook in self.hooks:
            try:
                hook(event, run)
            except Exception:
                log.exception("jobs.hook_failed", event=event, run_id=run.id)

    # ---- reads ---------------------------------------------------------------------------------

    async def get(self, run_id: int, *, conn: AsyncConnection[Any] | None = None) -> JobRun:
        async with self.transaction(conn) as c:
            row = await self._one(c, f"SELECT {SELECT} FROM job_runs WHERE id = %s", (run_id,))
        if row is None:
            raise JobNotFound(run_id)
        return JobRun.from_row(row)

    async def list(
        self,
        *,
        states: Sequence[str] | None = None,
        kind: str | None = None,
        subject: str | None = None,
        parent_id: int | None = None,
        before_id: int | None = None,
        limit: int = 50,
        conn: AsyncConnection[Any] | None = None,
    ) -> builtins.list[JobRun]:
        """Newest first; with ``parent_id``, only the runs that run queued."""
        clauses: builtins.list[str] = ["TRUE"]
        params: builtins.list[Any] = []
        if states:
            unknown = set(states) - set(STATES)
            if unknown:
                raise InvalidJob(f"unknown state(s) {', '.join(sorted(unknown))}")
            clauses.append("state = ANY(%s)")
            params.append(list(states))
        if kind is not None:
            clauses.append("kind = %s")
            params.append(kind)
        if subject is not None:
            clauses.append("subject = %s")
            params.append(subject)
        if parent_id is not None:
            clauses.append("parent_id = %s")
            params.append(parent_id)
        if before_id is not None:
            clauses.append("id < %s")
            params.append(before_id)
        async with self.transaction(conn) as c:
            rows = await self._all(
                c,
                f"SELECT {SELECT} FROM job_runs WHERE {' AND '.join(clauses)} ORDER BY id DESC LIMIT %s",
                (*params, limit),
            )
        return [JobRun.from_row(r) for r in rows]

    async def counts(
        self,
        *,
        kind: str | None = None,
        subject: str | None = None,
        since: dt.datetime | None = None,
        conn: AsyncConnection[Any] | None = None,
    ) -> dict[str, int]:
        """How many runs are in each state (every state, 0 when none). With ``since``, finished runs
        count only when they finished at or after it; active runs always count."""
        clauses: builtins.list[str] = ["TRUE"]
        params: builtins.list[Any] = []
        if kind is not None:
            clauses.append("kind = %s")
            params.append(kind)
        if subject is not None:
            clauses.append("subject = %s")
            params.append(subject)
        if since is not None:
            clauses.append("(state = ANY(%s) OR finished_at >= %s)")
            params += [list(ACTIVE), since]
        async with self.transaction(conn) as c:
            rows = await self._all(
                c, f"SELECT state, count(*) AS n FROM job_runs WHERE {' AND '.join(clauses)} GROUP BY state", params
            )
        found = {r["state"]: r["n"] for r in rows}
        return {state: found.get(state, 0) for state in STATES}

    async def related(
        self, run_id: int, *, limit: int = 200, conn: AsyncConnection[Any] | None = None
    ) -> tuple[int, builtins.list[JobRun], bool]:
        """The tree ``run_id`` is in: its root (the furthest ancestor) and every run under it, oldest first,
        at most ``limit``. Returns (root id, runs, whether some were left out). A run that nothing queued
        and that queued nothing comes back alone."""
        async with self.transaction(conn) as c:
            await self.get(run_id, conn=c)
            up = await self._one(
                c,
                "WITH RECURSIVE up(id, parent_id, depth) AS ("
                " SELECT id, parent_id, 0 FROM job_runs WHERE id = %s"
                " UNION ALL SELECT r.id, r.parent_id, up.depth + 1 FROM job_runs r JOIN up ON r.id = up.parent_id"
                " WHERE up.depth < 100)"
                " SELECT id FROM up ORDER BY depth DESC LIMIT 1",
                (run_id,),
            )
            root = up["id"] if up else run_id
            rows = await self._all(
                c,
                "WITH RECURSIVE down(id, depth) AS ("
                " SELECT id, 0 FROM job_runs WHERE id = %s"
                " UNION ALL SELECT r.id, down.depth + 1 FROM job_runs r JOIN down ON r.parent_id = down.id"
                " WHERE down.depth < 100)"
                f" SELECT {SELECT} FROM job_runs WHERE id IN (SELECT id FROM down) ORDER BY id LIMIT %s",
                (root, limit + 1),
            )
        runs = [JobRun.from_row(r) for r in rows]
        return root, runs[:limit], len(runs) > limit

    async def find(
        self,
        kind: str,
        subject: str | None = None,
        *,
        payload_contains: dict[str, Any] | None = None,
        states: Sequence[str] = ACTIVE,
        conn: AsyncConnection[Any] | None = None,
    ) -> builtins.list[JobRun]:
        """Runs of ``kind`` (newest first) on ``subject`` and/or whose payload contains the given keys:
        "is one already queued for this stream?"."""
        clauses: builtins.list[str] = ["kind = %s", "state = ANY(%s)"]
        params: builtins.list[Any] = [kind, list(states)]
        if subject is not None:
            clauses.append("subject = %s")
            params.append(subject)
        if payload_contains:
            clauses.append("payload @> %s")
            params.append(Jsonb(payload_contains))
        async with self.transaction(conn) as c:
            rows = await self._all(
                c, f"SELECT {SELECT} FROM job_runs WHERE {' AND '.join(clauses)} ORDER BY id DESC", params
            )
        return [JobRun.from_row(r) for r in rows]

    # ---- actions -------------------------------------------------------------------------------

    async def enqueue(
        self,
        kind: str,
        subject: str | None = None,
        payload: dict[str, Any] | None = None,
        *,
        actor: Actor = SYSTEM,
        step: str | None = None,
        pause_before: Sequence[str] | None = None,
        paused: bool = False,
        not_before: dt.datetime | None = None,
        queued_key: str | None = None,
        active_key: str | None = None,
        on_duplicate: OnDuplicate = "return",
        merge: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None,
        scope: str | None = None,
        parent_id: int | None = None,
        request_id: str | None = None,
        conn: AsyncConnection[Any] | None = None,
    ) -> Enqueued:
        """Queue a run at ``step`` (default: the kind's first). It starts ``paused`` when asked to, or
        when that step is one of its gates.

        Dedupe: while a run of ``kind`` with the same ``queued_key`` is queued, or one with the same
        ``active_key`` is queued, running or paused, ``on_duplicate`` decides: "return" it, "merge"
        this payload into a *queued* one with ``merge(old, new)``, or "raise" JobConflict.

        ``parent_id`` is the run that queued this one; an ``Actor("job", id=<run id>)`` sets it when not
        given (``StepContext.enqueue`` does both). A duplicate keeps the parent it had.
        """
        if parent_id is None and actor.kind == "job" and actor.id and actor.id.isdigit():
            parent_id = int(actor.id)
        job_kind = self.registry.get(kind)
        if step is not None:
            self.registry.check_steps(kind, [step])
        if pause_before is not None:
            self.registry.check_steps(kind, pause_before)
        first = step or job_kind.steps[0]
        gates = job_kind.pause_before if pause_before is None else tuple(pause_before)
        state = "paused" if paused or first in gates else "queued"
        payload = dict(payload or {})

        for _ in range(3):
            async with self.transaction(conn) as c:
                existing = await self._duplicate(c, kind, queued_key, active_key)
                if existing is None:
                    try:
                        async with c.transaction():
                            run = await self._insert(
                                c,
                                kind,
                                subject,
                                payload,
                                state,
                                first,
                                pause_before,
                                not_before,
                                queued_key,
                                active_key,
                                actor,
                                scope,
                                parent_id,
                            )
                    except errors.UniqueViolation:
                        continue  # a concurrent enqueue won: go and find its run
                    if state == "queued":
                        run = await self._defer(c, run)
                    after: dict[str, Any] = {"kind": kind, "subject": subject, "step": first, "state": state}
                    if parent_id is not None:
                        after["parent_id"] = parent_id
                    await self._audit(c, "job.enqueue", run, actor, request_id, scope, after=after)
                    created = True
                else:
                    run, created = await self._on_duplicate(
                        c, existing, payload, on_duplicate, merge, actor, request_id, scope
                    )
            if created:
                log.info("jobs.enqueued", run_id=run.id, kind=kind, subject=subject, state=state, actor=actor.label())
            return Enqueued(run, created)
        raise JobConflict(f"could not enqueue {kind!r}: its dedupe keys keep conflicting")

    async def _duplicate(
        self, conn: AsyncConnection[Any], kind: str, queued_key: str | None, active_key: str | None
    ) -> JobRun | None:
        if queued_key is None and active_key is None:
            return None
        row = await self._one(
            conn,
            f"SELECT {SELECT} FROM job_runs WHERE kind = %(kind)s AND ("
            " (state = 'queued' AND queued_key = %(q)s)"
            " OR (state IN ('queued', 'running', 'paused') AND active_key = %(a)s))"
            " ORDER BY id LIMIT 1 FOR UPDATE",
            {"kind": kind, "q": queued_key, "a": active_key},
        )
        return JobRun.from_row(row) if row else None

    async def _insert(
        self,
        conn: AsyncConnection[Any],
        kind: str,
        subject: str | None,
        payload: dict[str, Any],
        state: str,
        step: str,
        pause_before: Sequence[str] | None,
        not_before: dt.datetime | None,
        queued_key: str | None,
        active_key: str | None,
        actor: Actor,
        scope: str | None,
        parent_id: int | None = None,
    ) -> JobRun:
        row = await self._one(
            conn,
            "INSERT INTO job_runs (kind, subject, payload, state, step, pause_before, not_before, queued_key,"
            " active_key, actor_kind, actor_id, actor_login, via, scope, parent_id) VALUES (%s, %s, %s, %s, %s,"
            f" %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING {SELECT}",
            (
                kind,
                subject,
                Jsonb(payload),
                state,
                step,
                list(pause_before) if pause_before is not None else None,
                not_before,
                queued_key,
                active_key,
                actor.kind,
                actor.id,
                actor.login,
                actor.via,
                scope,
                parent_id,
            ),
        )
        assert row is not None
        return JobRun.from_row(row)

    async def _on_duplicate(
        self,
        conn: AsyncConnection[Any],
        existing: JobRun,
        payload: dict[str, Any],
        on_duplicate: OnDuplicate,
        merge: Callable[..., dict[str, Any]] | None,
        actor: Actor,
        request_id: str | None,
        scope: str | None,
    ) -> tuple[JobRun, bool]:
        if on_duplicate == "raise":
            raise JobConflict(f"run {existing.id} of {existing.kind!r} is already {existing.state}")
        if on_duplicate == "merge" and existing.state == "queued":
            if merge is None:
                raise ValueError("on_duplicate='merge' needs a merge function")
            merged = merge(dict(existing.payload), payload)
            if merged != existing.payload:
                run = await self._update(conn, existing.id, payload=merged)
                await self._audit(
                    conn, "job.merge", run, actor, request_id, scope, before=existing.payload, after=merged
                )
                return run, False
        return existing, False

    async def pause(
        self,
        run_id: int,
        *,
        actor: Actor = SYSTEM,
        request_id: str | None = None,
        conn: AsyncConnection[Any] | None = None,
    ) -> JobRun:
        """A queued run pauses now; a running one at its next step boundary (it stays ``running``
        with ``pause_next`` until then). Pausing a paused run changes nothing."""
        async with self.transaction(conn) as c:
            run = await self._lock_run(c, run_id)
            if run.state == "paused":
                return run
            if run.state == "queued":
                await self._drop_job(c, run)
                new = await self._update(c, run_id, state="paused", procrastinate_job_id=None)
            elif run.state == "running":
                new = await self._update(c, run_id, pause_next=True)
            else:
                raise JobConflict(f"run {run_id} is {run.state}; only queued or running runs can be paused")
            await self._audit(
                c,
                "job.pause",
                new,
                actor,
                request_id,
                before={"state": run.state},
                after={"state": new.state, "pause_next": new.pause_next},
            )
        return new

    async def resume(
        self,
        run_id: int,
        *,
        once: bool = False,
        actor: Actor = SYSTEM,
        request_id: str | None = None,
        conn: AsyncConnection[Any] | None = None,
    ) -> JobRun:
        """Queue a paused run at its step; ``once`` pauses it again after that step (single-stepping)."""
        async with self.transaction(conn) as c:
            run = await self._lock_run(c, run_id)
            if run.state != "paused":
                raise JobConflict(f"run {run_id} is {run.state}; only paused runs can be resumed")
            new = await self._update(c, run_id, state="queued", pause_next=once)
            new = await self._defer(c, new)
            await self._audit(
                c,
                "job.resume",
                new,
                actor,
                request_id,
                before={"state": "paused"},
                after={"state": "queued", "once": once},
            )
        return new

    async def retry(
        self,
        run_id: int,
        *,
        step: str | None = None,
        at: dt.datetime | None = None,
        pause_before: Sequence[str] | None | Literal["keep"] = "keep",
        actor: Actor = SYSTEM,
        request_id: str | None = None,
        conn: AsyncConnection[Any] | None = None,
    ) -> JobRun:
        """Queue a finished run again with fresh attempts: a failed or cancelled one at ``step`` or where
        it stopped, a succeeded one at ``step`` or its first. ``at`` (timezone-aware) schedules it for
        later; ``pause_before`` replaces its gates (None: back to the kind's) so it can stop before an
        earlier step this time; one on the step it starts at pauses it at once."""
        if at is not None and at.tzinfo is None:
            raise InvalidJob("retry needs a timezone-aware 'at'")
        async with self.transaction(conn) as c:
            run = await self._lock_run(c, run_id)
            if run.state not in FINISHED:
                raise JobConflict(f"run {run_id} is {run.state}; only finished runs can be retried")
            kind = self.registry.get(run.kind)
            if step is not None:
                self.registry.check_steps(run.kind, [step])
            first = step or (run.step if run.state != "succeeded" else None) or kind.steps[0]
            values: dict[str, Any] = {}
            if pause_before != "keep":
                if pause_before is not None:
                    self.registry.check_steps(run.kind, pause_before)
                    pause_before = list(pause_before)
                values["pause_before"] = pause_before
            # Only a gate passed now holds it at the step it starts at: retrying is a "go".
            hold = first in (values.get("pause_before") or ())
            try:
                async with c.transaction():
                    new = await self._update(
                        c,
                        run_id,
                        state="paused" if hold else "queued",
                        step=first,
                        attempts=0,
                        last_error=None,
                        not_before=at,
                        cancel_requested=False,
                        pause_next=False,
                        finished_at=None,
                        procrastinate_job_id=None,
                        **values,
                    )
            except errors.UniqueViolation:
                raise JobConflict(f"another run of {run.kind!r} with the same key is already active") from None
            if new.state == "queued":
                new = await self._defer(c, new)
            after: dict[str, Any] = {"state": new.state, "step": new.step}
            if at is not None:
                after["not_before"] = at.isoformat()
            if values:
                after["pause_before"] = values["pause_before"]
            await self._audit(c, "job.retry", new, actor, request_id, before={"state": run.state}, after=after)
        return new

    async def cancel(
        self,
        run_id: int,
        *,
        actor: Actor = SYSTEM,
        request_id: str | None = None,
        wait: float = 5.0,
        conn: AsyncConnection[Any] | None = None,
    ) -> JobRun:
        """End a queued or paused run now. A running run is asked to stop: "interrupt" kinds are
        cancelled mid-step, "cooperative" ones stop at their next ``should_stop()`` check. Waits up
        to ``wait`` seconds for it to reach ``cancelled`` and returns the run as it is then."""
        async with self.transaction(conn) as c:
            run = await self._lock_run(c, run_id)
            if run.state in ("queued", "paused"):
                await self._drop_job(c, run)
                new = await self._update(
                    c, run_id, state="cancelled", cancel_requested=True, finished_at=_now(), procrastinate_job_id=None
                )
            elif run.state == "running":
                new = await self._update(c, run_id, cancel_requested=True)
            else:
                raise JobConflict(f"run {run_id} is {run.state}; only queued, paused or running runs can be cancelled")
            await self._audit(c, "job.cancel", new, actor, request_id, before={"state": run.state})
        if new.state != "running":
            self._hook("cancelled", new)
            return new
        if run_id in self._tasks:
            self._cancelling.add(run_id)
        if self.registry.get(new.kind).cancel_mode == "interrupt" and new.procrastinate_job_id is not None:
            # The worker that runs it (in any process) cancels the step's task.
            await self.app.job_manager.cancel_job_by_id_async(new.procrastinate_job_id, abort=True)
        deadline = asyncio.get_running_loop().time() + wait
        while new.state == "running" and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.1)
            new = await self.get(run_id)
        return new

    async def update(
        self,
        run_id: int,
        *,
        pause_before: Sequence[str] | None | Literal["default"] = "default",
        pause_next: bool | None = None,
        actor: Actor = SYSTEM,
        request_id: str | None = None,
        conn: AsyncConnection[Any] | None = None,
    ) -> JobRun:
        """Change an active run's gates: ``pause_before`` (None: back to the kind's default) and/or
        ``pause_next``. They apply the next time the run moves on to a step."""
        values: dict[str, Any] = {}
        async with self.transaction(conn) as c:
            run = await self._lock_run(c, run_id)
            if not run.active:
                raise JobConflict(f"run {run_id} is {run.state}; only active runs can be changed")
            if pause_before != "default":
                if pause_before is not None:
                    self.registry.check_steps(run.kind, pause_before)
                    pause_before = list(pause_before)
                values["pause_before"] = pause_before
            if pause_next is not None:
                values["pause_next"] = pause_next
            if not values:
                return run
            new = await self._update(c, run_id, **values)
            await self._audit(
                c, "job.update", new, actor, request_id, before={k: getattr(run, k) for k in values}, after=values
            )
        return new

    # ---- recovery ------------------------------------------------------------------------------

    async def reconcile(self) -> int:
        """Bring ``job_runs`` and procrastinate back in line after a crash or a lost job:
        a ``running`` run whose job is gone, finished or stalled goes back to ``queued``, and a
        ``queued`` run without a waiting job (or whose job is stalled) gets a new one, and a stalled
        ``vex.run`` job that is no longer its run's current one is ended so it stops holding its
        lock. Returns how many runs it touched."""
        touched = 0
        async with self.transaction() as c:
            rows = await self._all(
                c,
                f"SELECT {', '.join('r.' + col for col in SELECT.split(', '))}, j.status::text AS job_status"
                " FROM job_runs r LEFT JOIN procrastinate_jobs j ON j.id = r.procrastinate_job_id"
                " WHERE r.state IN ('queued', 'running') FOR UPDATE OF r SKIP LOCKED",
            )
            for row in rows:
                run = JobRun.from_row(row)
                status = row["job_status"]
                if run.id in self._tasks:
                    continue
                if status == "doing":
                    assert run.procrastinate_job_id is not None  # the join found its job
                    if not await self._finish_stalled(c, run.procrastinate_job_id):
                        continue  # a live worker has it
                    log.warning("jobs.stalled_job_ended", run_id=run.id, job_id=run.procrastinate_job_id)
                elif status == "todo" and run.state == "queued":
                    continue
                elif status is None and run.state == "queued" and self._scheduled(run):
                    continue  # its time is past the next reconcile
                if run.state == "running":
                    run = await self._update(c, run.id, state="queued")
                    log.warning("jobs.recovered", run_id=run.id, kind=run.kind, step=run.step)
                await self._defer(c, run)
                touched += 1
            # Stalled jobs no active run points at any more (a resume deferred a newer one): their
            # lock would keep the newer job from ever being fetched.
            orphans = await self._all(
                c,
                "DELETE FROM procrastinate_jobs j WHERE j.task_name = %s AND j.status = 'doing'"
                " AND NOT EXISTS (SELECT 1 FROM procrastinate_workers w WHERE w.id = j.worker_id"
                " AND w.last_heartbeat >= now() - make_interval(secs => %s))"
                " AND NOT EXISTS (SELECT 1 FROM job_runs r WHERE r.procrastinate_job_id = j.id"
                " AND r.state IN ('queued', 'running')) RETURNING j.id, j.lock",
                (TASK_NAME, STALLED_AFTER),
            )
            for orphan in orphans:
                log.warning("jobs.orphan_job_ended", job_id=orphan["id"], lock=orphan["lock"])
        return touched

    async def _reconcile_forever(self) -> None:
        while True:
            await asyncio.sleep(self.reconcile_interval)
            try:
                await self.reconcile()
            except Exception:
                log.exception("jobs.reconcile_failed")

    # ---- execution -----------------------------------------------------------------------------

    def _cancel_local(self, run_id: int) -> bool:
        return run_id in self._cancelling

    async def _cancel_requested(self, run_id: int) -> bool:
        async with self.transaction() as c:
            row = await self._one(c, "SELECT cancel_requested FROM job_runs WHERE id = %s", (run_id,))
        return bool(row and row["cancel_requested"])

    async def _save(self, ctx: StepContext) -> None:
        async with self.transaction() as c:
            await self._update(c, ctx.run_id, payload=ctx.payload, subject=ctx.subject)

    async def _audit_step(self, entry: AuditEntry) -> int | None:
        if self.audit_table is None:
            return None
        async with self.transaction() as c:
            return await audit_pg.record(c, entry, table=self.audit_table)

    async def _execute(self, context: procrastinate.JobContext, run_id: int) -> None:
        job_id = context.job.id
        try:
            run = await self.get(run_id)
        except JobNotFound:
            return
        if run.state != "queued" or run.procrastinate_job_id != job_id:
            log.info("jobs.stale_job", run_id=run_id, job_id=job_id, state=run.state)
            return
        kind = self.registry.kinds.get(run.kind)
        # The worker took this job only once a run slot was free (_Slots) and frees it when this returns.
        try:
            async with self.transaction() as c:
                row = await self._one(
                    c,
                    "UPDATE job_runs SET state = 'running', started_at = COALESCE(started_at, now()),"
                    " not_before = NULL WHERE id = %s AND state = 'queued' AND procrastinate_job_id = %s"
                    f" RETURNING {SELECT}",
                    (run_id, job_id),
                )
            if row is None:
                return  # paused or cancelled since it was fetched
            run = JobRun.from_row(row)
            if kind is None:
                async with self.transaction() as c:
                    run = await self._update(
                        c, run_id, state="failed", finished_at=_now(), last_error=f"unknown job kind {run.kind!r}"
                    )
                self._hook("failed", run)
                return
            self._tasks[run_id] = asyncio.current_task()  # type: ignore[assignment]
            with structlog.contextvars.bound_contextvars(run_id=run_id, kind=run.kind):
                await self._run_steps(run, kind)
        finally:
            self._tasks.pop(run_id, None)
            self._cancelling.discard(run_id)

    async def _run_steps(self, run: JobRun, kind: JobKind) -> None:
        ctx = StepContext(self, run, kind)
        arg = self.context_factory(ctx) if self.context_factory else ctx
        steps = kind.steps
        start = steps.index(run.step) if run.step in steps else 0
        # The step to resume from: advanced as soon as a step returns, so the error paths below
        # record progress even if the checkpoint write itself failed.
        current: str | None = steps[start]
        ctx.step = current
        ctx.log.info("running %s from step %s (attempt %d)", kind.name, current, ctx.attempt)
        self._hook("started", run)
        try:
            i = start
            while i < len(steps):
                ctx.step = steps[i]
                ctx.log.info("step %s", steps[i])
                try:
                    await self.registry.steps[steps[i]](arg)
                    i += 1
                except SkipTo as skip:
                    i = self._skip_to(steps, i, skip)
                    ctx.log.info("%s; %s", skip, f"skipping to step {skip.step}" if skip.step else "nothing left to do")
                current = steps[i] if i < len(steps) else None
                if current is None:
                    break
                outcome = await self._advance(ctx, current)
                if outcome == "stop":
                    raise RunStopped()
                if outcome == "pause":
                    ctx.step = current
                    ctx.log.info("paused before step %s", current)
                    self._hook("paused", ctx.run)
                    return
            async with self.transaction() as c:
                done = await self._update(
                    c,
                    run.id,
                    state="succeeded",
                    step=None,
                    payload=ctx.payload,
                    subject=ctx.subject,
                    last_error=None,
                    not_before=None,
                    pause_next=False,
                    finished_at=_now(),
                )
            ctx.step = None
            ctx.log.info("%s finished", kind.name)
            self._hook("succeeded", done)
        except (asyncio.CancelledError, RunStopped) as exc:
            await asyncio.shield(self._stopped(ctx, current))
            if isinstance(exc, asyncio.CancelledError):
                raise
        except Exception as exc:
            await self._failed(ctx, kind, current, exc)

    @staticmethod
    def _skip_to(steps: Sequence[str], i: int, skip: SkipTo) -> int:
        """The index ``skip`` moves the run to from step ``i``: its step, which must come later, or the end."""
        if skip.step is None:
            return len(steps)
        if skip.step not in steps[i + 1 :]:
            raise StepRefused(f"{steps[i]} skipped to {skip.step!r}, which is not a later step of this kind")
        return steps.index(skip.step, i + 1)

    async def _advance(self, ctx: StepContext, step: str) -> Literal["go", "pause", "stop"]:
        """Checkpoint ``step`` as the next one; then stop (cancel requested), pause (a gate) or go."""
        async with self.transaction() as c:
            run = await self._lock_run(c, ctx.run_id)
            await self._update(c, ctx.run_id, step=step, payload=ctx.payload, subject=ctx.subject)
            if run.cancel_requested:
                return "stop"
            # The registry's kind, not ctx.kind: its default gates may have changed since the run started.
            kind = self.registry.kinds.get(run.kind, ctx.kind)
            gates = kind.pause_before if run.pause_before is None else run.pause_before
            if run.pause_next or step in gates:
                ctx.run = await self._update(c, ctx.run_id, state="paused", pause_next=False, procrastinate_job_id=None)
                return "pause"
        return "go"

    async def _stopped(self, ctx: StepContext, current: str | None) -> None:
        """The step was interrupted: a requested cancel ends the run, anything else (a shutdown)
        queues it again at the unfinished step."""
        try:
            async with self.transaction() as c:
                run = await self._lock_run(c, ctx.run_id)
                if run.cancel_requested:
                    run = await self._update(
                        c,
                        run.id,
                        state="cancelled",
                        step=current,
                        payload=ctx.payload,
                        subject=ctx.subject,
                        finished_at=_now(),
                    )
                    ctx.log.info("cancelled")
                    event = "cancelled"
                else:
                    run = await self._update(
                        c, run.id, state="queued", step=current, payload=ctx.payload, subject=ctx.subject
                    )
                    run = await self._defer(c, run)
                    ctx.log.info("interrupted by shutdown; will resume at step %s", current)
                    event = "requeued"
            self._hook(event, run)
        except Exception:
            # The pool may already be closing: reconcile() re-queues the run on the next start.
            log.exception("jobs.stop_write_failed", run_id=ctx.run_id)

    async def _failed(self, ctx: StepContext, kind: JobKind, current: str | None, exc: BaseException) -> None:
        max_attempts = kind.max_attempts or self.max_attempts
        err = str(exc) if isinstance(exc, StepError) else "".join(traceback.format_exception(exc))[-4000:]
        async with self.transaction() as c:
            run = await self._lock_run(c, ctx.run_id)
            attempts = run.attempts + 1
            common = {
                "step": current,
                "attempts": attempts,
                "last_error": err,
                "payload": ctx.payload,
                "subject": ctx.subject,
            }
            if run.cancel_requested:
                run = await self._update(c, run.id, state="cancelled", finished_at=_now(), **common)
                event = "cancelled"
            elif isinstance(exc, StepDeferred):
                run = await self._update(
                    c, run.id, state="queued", not_before=max(exc.until, _now()), **{**common, "attempts": 0}
                )
                run = await self._defer(c, run, hold=True)
                ctx.log.warning("step %s deferred until %s: %s", current, exc.until.isoformat(), exc)
                event = "deferred"
            elif isinstance(exc, StepRefused) or attempts >= max_attempts:
                run = await self._update(c, run.id, state="failed", finished_at=_now(), **common)
                ctx.log.error("failed: %s", exc)
                event = "failed"
            else:
                delay = kind.retry_delay(attempts)
                run = await self._update(
                    c, run.id, state="queued", not_before=_now() + dt.timedelta(seconds=delay), **common
                )
                run = await self._defer(c, run, hold=True)
                ctx.log.warning("step %s failed (%s); retry %d in %gs", current, exc, attempts, delay)
                event = "retrying"
        self._hook(event, run)


def kinds_info(registry: Registry) -> Iterable[dict[str, Any]]:
    for kind in registry.kinds.values():
        yield {
            "name": kind.name,
            "description": kind.description,
            "steps": list(kind.steps),
            "pause_before": list(kind.pause_before),
            "cancel_mode": kind.cancel_mode,
            "max_attempts": kind.max_attempts,
        }
