from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from typing import Any

import psycopg
import pytest

from vex_platform import migrations
from vex_platform.jobs import JobRun, JobRuntime, Registry

if sys.platform == "win32":
    # psycopg's async connections need a selector loop on Windows.
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# VEX_TEST_DSN wins; CI's shared Postgres service sets TEST_DATABASE_URL; the default is compose.yaml's.
DSN = (
    os.environ.get("VEX_TEST_DSN")
    or os.environ.get("TEST_DATABASE_URL")
    or "postgresql://vex:vex@127.0.0.1:55434/vex_platform_test"
)
AUDIT = "public.audit_log"


@pytest.fixture(scope="session")
def dsn() -> str:
    """A database with the jobs schema and the audit table freshly created from the packaged SQL."""
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS jobs CASCADE")
        conn.execute("DROP TABLE IF EXISTS public.audit_log")
        conn.execute(migrations.jobs_sql(1, schema="jobs"))
        conn.execute(migrations.jobs_sql(2, schema="jobs"))
        conn.execute(migrations.jobs_sql(3, schema="jobs"))
        conn.execute(migrations.jobs_sql(4, schema="jobs"))
        conn.execute(migrations.audit_sql(1, table=AUDIT))
    return DSN


@pytest.fixture(autouse=True)
def clean(request: pytest.FixtureRequest) -> None:
    if "dsn" not in request.fixturenames:
        return
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            "TRUNCATE jobs.job_runs, jobs.job_run_events, jobs.procrastinate_jobs, jobs.procrastinate_workers,"
            " public.audit_log RESTART IDENTITY CASCADE"
        )


MakeRuntime = Callable[..., Awaitable[JobRuntime]]


@pytest.fixture
async def make_runtime(dsn: str) -> AsyncIterator[MakeRuntime]:
    """``await make_runtime(registry, start=True, **options)``: an open runtime, closed after the test."""
    made: list[JobRuntime] = []

    async def make(registry: Registry, *, start: bool = True, **options: Any) -> JobRuntime:
        options.setdefault("audit_table", AUDIT)
        options.setdefault("poll_interval", 0.2)
        options.setdefault("shutdown_timeout", 2.0)
        runtime = JobRuntime(registry, dsn, **options)
        await runtime.open()
        made.append(runtime)
        if start:
            await runtime.start()
        return runtime

    yield make
    for runtime in made:
        await runtime.close()


async def wait_for(runtime: JobRuntime, run_id: int, states: Iterable[str], timeout: float = 15.0) -> JobRun:
    states = tuple(states)
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        run = await runtime.get(run_id)
        if run.state in states:
            return run
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"run {run_id} still {run.state} ({run.last_error}); wanted {states}")
        await asyncio.sleep(0.05)


async def until(check: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.05)


async def audit_rows(action_prefix: str = "") -> list[dict[str, Any]]:
    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        cur = conn.cursor(row_factory=psycopg.rows.dict_row)
        await cur.execute("SELECT * FROM public.audit_log WHERE action LIKE %s ORDER BY id", (action_prefix + "%",))
        return list(await cur.fetchall())
