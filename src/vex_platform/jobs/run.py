"""A job run as read from ``job_runs``."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Literal

from ..actor import Actor

State = Literal["queued", "running", "paused", "succeeded", "failed", "cancelled"]
STATES: tuple[str, ...] = ("queued", "running", "paused", "succeeded", "failed", "cancelled")
ACTIVE: tuple[str, ...] = ("queued", "running", "paused")
FINISHED: tuple[str, ...] = ("succeeded", "failed", "cancelled")

COLUMNS = (
    "id",
    "kind",
    "subject",
    "state",
    "step",
    "payload",
    "attempts",
    "last_error",
    "not_before",
    "pause_before",
    "pause_next",
    "cancel_requested",
    "queued_key",
    "active_key",
    "actor_kind",
    "actor_id",
    "actor_login",
    "via",
    "procrastinate_job_id",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
    "scope",
    "parent_id",
    "dismissed_at",
    "schedule_id",
)
SELECT = ", ".join(COLUMNS)


@dataclass
class JobRun:
    id: int
    kind: str
    subject: str | None
    state: State
    step: str | None
    payload: dict[str, Any]
    attempts: int
    last_error: str | None
    not_before: dt.datetime | None
    pause_before: list[str] | None
    pause_next: bool
    cancel_requested: bool
    queued_key: str | None
    active_key: str | None
    actor: Actor
    procrastinate_job_id: int | None
    created_at: dt.datetime
    updated_at: dt.datetime
    started_at: dt.datetime | None
    finished_at: dt.datetime | None
    scope: str | None = None
    parent_id: int | None = None
    dismissed_at: dt.datetime | None = None
    schedule_id: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> JobRun:
        values = {k: row[k] for k in COLUMNS if k not in ("actor_kind", "actor_id", "actor_login", "via")}
        values["payload"] = dict(values["payload"] or {})
        return cls(
            **values,
            actor=Actor(row["actor_kind"], row["actor_id"], row["actor_login"], row["via"]),
        )

    @property
    def active(self) -> bool:
        return self.state in ACTIVE
