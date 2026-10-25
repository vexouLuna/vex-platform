"""Per-run event log: every ``ctx.log`` line, step change and progress report (``job_run_events``).

Events are buffered in memory and written in batches (``run_forever`` flushes about once a second),
so logging never waits on the database. Each run keeps roughly its newest ``cap`` log lines; older ones
are pruned as new ones arrive. Progress reports don't count toward the cap: only the latest one per run and
step is kept. ``add`` is thread-safe: progress may arrive from a worker thread.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections import defaultdict, deque
from typing import Any, Literal

import structlog
from psycopg.errors import CheckViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

log = structlog.get_logger(__name__)

Level = Literal["debug", "info", "warning", "error"]
LEVELS = ("debug", "info", "warning", "error")
UNITS = ("items", "parts", "bytes", "percent", "seconds")
MAX_PER_RUN = 20_000  # log lines; progress reports are kept apart (one per run and step)
PRUNE_EVERY = 100  # log lines per run between prunes

_PRUNE = """
DELETE FROM job_run_events WHERE run_id = %(run_id)s AND progress IS NULL AND id < (
    SELECT id FROM job_run_events WHERE run_id = %(run_id)s AND progress IS NULL
    ORDER BY id DESC OFFSET %(keep)s LIMIT 1
)
"""
# Keeps only the newest progress report of a run's step.
_PRUNE_PROGRESS = """
DELETE FROM job_run_events WHERE run_id = %(run_id)s AND step IS NOT DISTINCT FROM %(step)s
    AND progress IS NOT NULL AND id < (
        SELECT max(id) FROM job_run_events
        WHERE run_id = %(run_id)s AND step IS NOT DISTINCT FROM %(step)s AND progress IS NOT NULL
    )
"""


def progress(done: float, total: float | None = None, unit: str = "items") -> dict[str, Any]:
    if unit not in UNITS:
        raise ValueError(f"unknown progress unit {unit!r}; units: {', '.join(UNITS)}")
    return {"done": done, "total": total, "unit": unit}


class RunEvents:
    def __init__(self, pool: AsyncConnectionPool, cap: int = MAX_PER_RUN) -> None:
        self.pool = pool
        self.cap = cap
        self._pending: deque[dict[str, Any]] = deque(maxlen=50_000)  # bounded if the database is down
        self._since_prune: defaultdict[int, int] = defaultdict(int)
        self._lock = asyncio.Lock()
        # False once the database refused a debug line: the app hasn't applied ``jobs_sql(4)`` yet.
        self.debug_ok = True

    def add(
        self,
        run_id: int,
        level: Level,
        step: str | None,
        message: str,
        progress: dict[str, Any] | None = None,
        *,
        at: dt.datetime | None = None,
    ) -> None:
        if level == "debug" and not self.debug_ok:
            return
        # deque.append is atomic, so a thread may call this.
        self._pending.append(
            {
                "run_id": run_id,
                "at": at or dt.datetime.now(dt.UTC),
                "level": level if level in LEVELS else "info",
                "step": step,
                "message": message,
                "progress": Jsonb(progress) if progress is not None else None,
            }
        )

    async def flush(self) -> None:
        async with self._lock:
            rows = []
            while self._pending:
                rows.append(self._pending.popleft())
            if not rows:
                return
            # Only the last progress report per run and step in a batch is worth writing.
            last = {(r["run_id"], r["step"]): i for i, r in enumerate(rows) if r["progress"] is not None}
            rows = [r for i, r in enumerate(rows) if r["progress"] is None or last[(r["run_id"], r["step"])] == i]
            try:
                try:
                    await self._write(rows)
                except CheckViolation:
                    self.debug_ok = False
                    log.warning("jobs.debug_events_dropped", hint="apply migrations.jobs_sql(4) to store debug lines")
                    await self._write([r for r in rows if r["level"] != "debug"])
            except Exception as exc:
                # Events of a run whose row was deleted fail the whole batch: drop them, keep going.
                log.warning("jobs.events_dropped", count=len(rows), error=str(exc))

    async def _write(self, rows: list[dict[str, Any]]) -> None:
        async with self.pool.connection() as conn, conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO job_run_events (run_id, at, level, step, message, progress)"
                " VALUES (%(run_id)s, %(at)s, %(level)s, %(step)s, %(message)s, %(progress)s)",
                rows,
            )
            for key in {(r["run_id"], r["step"]) for r in rows if r["progress"] is not None}:
                await cur.execute(_PRUNE_PROGRESS, {"run_id": key[0], "step": key[1]})
            for row in rows:
                if row["progress"] is None:
                    self._since_prune[row["run_id"]] += 1
            for run_id in [r for r, n in self._since_prune.items() if n >= PRUNE_EVERY]:
                await cur.execute(_PRUNE, {"run_id": run_id, "keep": self.cap - 1})
                del self._since_prune[run_id]

    async def run_forever(self, interval: float = 1.0) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                await self.flush()
        finally:
            await asyncio.shield(self.flush())

    async def list(
        self,
        run_id: int,
        *,
        after: int = 0,
        before: int | None = None,
        newest: bool = False,
        min_level: Level = "debug",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Events at ``min_level`` or above, oldest first: those with id > ``after``; or, with ``before``,
        the ``limit`` just before that id; or, with ``newest``, the run's last ``limit``. Flushes first so a
        line logged a moment ago shows."""
        await self.flush()
        levels = list(LEVELS[LEVELS.index(min_level) :])
        backwards = newest or before is not None
        where = "id < %s" if before is not None else "id > %s"
        bound = before if before is not None else (0 if newest else after)
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, at, level, step, message, progress FROM job_run_events"
                f" WHERE run_id = %s AND {where} AND level = ANY(%s)"
                f" ORDER BY id {'DESC' if backwards else ''} LIMIT %s",
                (run_id, bound, levels, limit),
            )
            rows = list(await cur.fetchall())
        return rows[::-1] if backwards else rows
