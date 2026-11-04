"""Recurring runs: a schedule enqueues a run of its kind whenever its cron expression is due.

Cron is the five-field form, evaluated in UTC. A paused schedule skips its turns, and resuming does not
run the ones it missed. ``JobRuntime`` mixes this in; a tick (``run_schedules``) every
``schedule_interval`` seconds enqueues what is due.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

from croniter import croniter
from psycopg import AsyncConnection
from psycopg.types.json import Jsonb

from ..actor import SYSTEM, Actor
from ..audit import psycopg as audit_pg
from ..audit.model import AuditEntry
from .errors import InvalidJob, JobConflict, ScheduleNotFound

COLUMNS = (
    "id, name, kind, subject, payload, cron, paused_at, last_run_at, next_run_at,"
    " actor_kind, actor_id, actor_login, via, created_at, updated_at"
)
SCHEDULER = Actor("system", "scheduler", via="system")


@dataclass
class Schedule:
    id: int
    name: str
    kind: str
    subject: str | None
    payload: dict[str, Any]
    cron: str
    paused_at: dt.datetime | None
    last_run_at: dt.datetime | None
    next_run_at: dt.datetime
    actor: Actor
    created_at: dt.datetime
    updated_at: dt.datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Schedule:
        values = {k: v for k, v in row.items() if k not in ("actor_kind", "actor_id", "actor_login", "via")}
        values["payload"] = dict(values["payload"] or {})
        return cls(**values, actor=Actor(row["actor_kind"], row["actor_id"], row["actor_login"], row["via"]))

    @property
    def paused(self) -> bool:
        return self.paused_at is not None


def next_after(cron: str, after: dt.datetime) -> dt.datetime:
    """The first time after ``after`` the expression is due (UTC); InvalidJob when it is not valid cron."""
    if len(cron.split()) != 5 or not croniter.is_valid(cron):
        raise InvalidJob(f"{cron!r} is not a five-field cron expression")
    nxt: dt.datetime = croniter(cron, after.astimezone(dt.UTC)).get_next(dt.datetime)
    return nxt


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


# The mixin leans on JobRuntime's transaction, _one, _all, enqueue, registry and audit_table; mypy
# cannot see them from here, hence the ignores.
class SchedulesMixin:
    schedulable: frozenset[str]
    audit_table: str | None

    async def list_schedules(self) -> list[Schedule]:
        async with self.transaction() as c:  # type: ignore[attr-defined]
            rows = await self._all(c, f"SELECT {COLUMNS} FROM job_schedules ORDER BY id")  # type: ignore[attr-defined]
        return [Schedule.from_row(r) for r in rows]

    async def get_schedule(self, schedule_id: int) -> Schedule:
        async with self.transaction() as c:  # type: ignore[attr-defined]
            row = await self._one(c, f"SELECT {COLUMNS} FROM job_schedules WHERE id = %s", (schedule_id,))  # type: ignore[attr-defined]
        if row is None:
            raise ScheduleNotFound(schedule_id)
        return Schedule.from_row(row)

    async def _audit_schedule(
        self,
        conn: AsyncConnection[Any],
        verb: str,
        schedule: Schedule,
        actor: Actor,
        request_id: str | None,
        before: Any = None,
        after: Any = None,
    ) -> None:
        if self.audit_table is None:
            return
        entry = AuditEntry(
            f"schedule.{verb}",
            actor=actor,
            target=f"schedule:{schedule.id}",
            before=before,
            after=after,
            request_id=request_id,
        )
        await audit_pg.record(conn, entry, table=self.audit_table)

    def _check_schedule(self, kind: str, cron: str, *, seeded: bool = False) -> dt.datetime:
        self.registry.get(kind)  # type: ignore[attr-defined]
        if not seeded and kind not in self.schedulable:
            raise InvalidJob(f"{kind!r} cannot be scheduled; schedulable kinds: {', '.join(sorted(self.schedulable))}")
        return next_after(cron, _now())

    async def _insert_schedule(
        self,
        c: AsyncConnection[Any],
        name: str,
        kind: str,
        cron: str,
        subject: str | None,
        payload: dict[str, Any],
        nxt: dt.datetime,
        actor: Actor,
    ) -> Schedule:
        row = await self._one(  # type: ignore[attr-defined]
            c,
            "INSERT INTO job_schedules (name, kind, subject, payload, cron, next_run_at, actor_kind, actor_id,"
            f" actor_login, via) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING {COLUMNS}",
            (name, kind, subject, Jsonb(payload), cron, nxt, actor.kind, actor.id, actor.login, actor.via),
        )
        assert row is not None
        return Schedule.from_row(row)

    async def create_schedule(
        self,
        name: str,
        kind: str,
        cron: str,
        *,
        subject: str | None = None,
        payload: dict[str, Any] | None = None,
        actor: Actor = SYSTEM,
        request_id: str | None = None,
    ) -> Schedule:
        nxt = self._check_schedule(kind, cron)
        async with self.transaction() as c:  # type: ignore[attr-defined]
            if await self._one(c, "SELECT 1 FROM job_schedules WHERE name = %s", (name,)):  # type: ignore[attr-defined]
                raise JobConflict(f"a schedule named {name!r} exists")
            schedule = await self._insert_schedule(c, name, kind, cron, subject, payload or {}, nxt, actor)
            await self._audit_schedule(
                c, "create", schedule, actor, request_id, after={"name": name, "kind": kind, "cron": cron}
            )
        return schedule

    async def ensure_schedule(
        self,
        name: str,
        kind: str,
        cron: str,
        *,
        subject: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Schedule:
        """For an application's startup: create the schedule ``name`` if it is missing and leave an existing
        one alone, so what an admin changed (cron, paused) survives a restart. Any registered kind."""
        nxt = self._check_schedule(kind, cron, seeded=True)
        async with self.transaction() as c:  # type: ignore[attr-defined]
            row = await self._one(c, f"SELECT {COLUMNS} FROM job_schedules WHERE name = %s", (name,))  # type: ignore[attr-defined]
            if row is not None:
                return Schedule.from_row(row)
            return await self._insert_schedule(c, name, kind, cron, subject, payload or {}, nxt, SCHEDULER)

    async def _lock_schedule(self, c: AsyncConnection[Any], schedule_id: int) -> Schedule:
        row = await self._one(c, f"SELECT {COLUMNS} FROM job_schedules WHERE id = %s FOR UPDATE", (schedule_id,))  # type: ignore[attr-defined]
        if row is None:
            raise ScheduleNotFound(schedule_id)
        return Schedule.from_row(row)

    async def update_schedule(
        self,
        schedule_id: int,
        *,
        cron: str | None = None,
        subject: str | None = None,
        set_subject: bool = False,
        payload: dict[str, Any] | None = None,
        actor: Actor = SYSTEM,
        request_id: str | None = None,
    ) -> Schedule:
        """Change the cron expression, subject or payload; the name and kind stay."""
        async with self.transaction() as c:  # type: ignore[attr-defined]
            old = await self._lock_schedule(c, schedule_id)
            sets: dict[str, Any] = {}
            if cron is not None:
                sets["cron"] = cron
                sets["next_run_at"] = next_after(cron, _now())
            if set_subject:
                sets["subject"] = subject
            if payload is not None:
                sets["payload"] = Jsonb(payload)
            if not sets:
                return old
            cols = ", ".join(f"{k} = %({k})s" for k in sets)
            row = await self._one(  # type: ignore[attr-defined]
                c,
                f"UPDATE job_schedules SET {cols}, updated_at = now() WHERE id = %(_id)s RETURNING {COLUMNS}",
                {**sets, "_id": schedule_id},
            )
            assert row is not None
            new = Schedule.from_row(row)
            await self._audit_schedule(
                c, "update", new, actor, request_id, before={"cron": old.cron}, after={"cron": new.cron}
            )
        return new

    async def delete_schedule(self, schedule_id: int, *, actor: Actor = SYSTEM, request_id: str | None = None) -> None:
        """Remove it; the runs it made stay, with no schedule."""
        async with self.transaction() as c:  # type: ignore[attr-defined]
            old = await self._lock_schedule(c, schedule_id)
            await c.execute("DELETE FROM job_schedules WHERE id = %s", (schedule_id,))
            await self._audit_schedule(c, "delete", old, actor, request_id, before={"name": old.name, "kind": old.kind})

    async def pause_schedule(
        self, schedule_id: int, *, actor: Actor = SYSTEM, request_id: str | None = None
    ) -> Schedule:
        async with self.transaction() as c:  # type: ignore[attr-defined]
            old = await self._lock_schedule(c, schedule_id)
            if old.paused:
                return old
            row = await self._one(  # type: ignore[attr-defined]
                c,
                f"UPDATE job_schedules SET paused_at = now(), updated_at = now() WHERE id = %s RETURNING {COLUMNS}",
                (schedule_id,),
            )
            assert row is not None
            new = Schedule.from_row(row)
            await self._audit_schedule(c, "pause", new, actor, request_id)
        return new

    async def resume_schedule(
        self, schedule_id: int, *, actor: Actor = SYSTEM, request_id: str | None = None
    ) -> Schedule:
        """Un-pause it. Turns missed while paused are skipped: the next run is the next cron time from now."""
        async with self.transaction() as c:  # type: ignore[attr-defined]
            old = await self._lock_schedule(c, schedule_id)
            if not old.paused:
                return old
            row = await self._one(  # type: ignore[attr-defined]
                c,
                "UPDATE job_schedules SET paused_at = NULL, next_run_at = %s, updated_at = now() WHERE id = %s"
                f" RETURNING {COLUMNS}",
                (next_after(old.cron, _now()), schedule_id),
            )
            assert row is not None
            new = Schedule.from_row(row)
            await self._audit_schedule(c, "resume", new, actor, request_id)
        return new

    async def run_schedule(self, schedule_id: int, *, actor: Actor = SYSTEM, request_id: str | None = None) -> Any:
        """Enqueue its run now, paused or not; the cron timing is unchanged. Returns the ``Enqueued``."""
        async with self.transaction() as c:  # type: ignore[attr-defined]
            schedule = await self._lock_schedule(c, schedule_id)
            result = await self._enqueue_scheduled(c, schedule, actor, request_id)
            await self._audit_schedule(
                c, "run_now", schedule, actor, request_id, after={"run_id": result.run.id, "created": result.created}
            )
        return result

    async def _enqueue_scheduled(
        self, c: AsyncConnection[Any], schedule: Schedule, actor: Actor, request_id: str | None
    ) -> Any:
        result = await self.enqueue(  # type: ignore[attr-defined]
            schedule.kind,
            schedule.subject,
            schedule.payload,
            actor=actor,
            active_key=f"schedule:{schedule.id}",  # a run still going blocks the next one
            schedule_id=schedule.id,
            request_id=request_id,
            conn=c,
        )
        if result.created:
            await c.execute("UPDATE job_schedules SET last_run_at = now() WHERE id = %s", (schedule.id,))
        return result

    async def run_schedules(self) -> int:
        """One tick: enqueue every unpaused schedule that is due and move each due one (paused too) to its
        next time after now. Returns how many runs it made."""
        made = 0
        async with self.transaction() as c:  # type: ignore[attr-defined]
            rows = await self._all(  # type: ignore[attr-defined]
                c,
                f"SELECT {COLUMNS} FROM job_schedules WHERE next_run_at <= now() ORDER BY next_run_at"
                " FOR UPDATE SKIP LOCKED",
            )
            for row in rows:
                schedule = Schedule.from_row(row)
                if not schedule.paused:
                    made += (await self._enqueue_scheduled(c, schedule, SCHEDULER, None)).created
                await c.execute(
                    "UPDATE job_schedules SET next_run_at = %s WHERE id = %s",
                    (next_after(schedule.cron, _now()), schedule.id),
                )
        return made
