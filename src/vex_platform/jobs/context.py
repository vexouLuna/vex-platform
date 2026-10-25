"""What a step function receives: the run's payload, a log that feeds the run's event log, progress
reports, checkpoints and the stop signal."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import structlog

from ..actor import Actor
from ..audit.model import AuditEntry
from .errors import RunStopped
from .events import Level, progress

if TYPE_CHECKING:
    from .registry import JobKind
    from .run import JobRun
    from .runtime import Enqueued, JobRuntime

_logger = structlog.get_logger("vex_platform.jobs.run")


class RunLog:
    """``ctx.log.info("uploaded %d parts", n)``: %-style like stdlib logging. Each line goes to the
    run's event log and to structlog as ``jobs.log`` with run_id, kind and step bound."""

    def __init__(self, ctx: StepContext) -> None:
        self._ctx = ctx

    def _emit(
        self, level: Level, msg: str, args: tuple[Any, ...], progress: dict[str, Any] | None = None, **fields: Any
    ) -> None:
        text = msg % args if args else msg
        ctx = self._ctx
        ctx.runtime.events.add(ctx.run_id, level, ctx.step, text, progress)
        method = {"debug": _logger.debug, "info": _logger.info, "warning": _logger.warning, "error": _logger.error}[
            level
        ]
        method("jobs.log", message=text, run_id=ctx.run_id, kind=ctx.kind.name, step=ctx.step, **fields)

    def debug(self, msg: str, *args: Any) -> None:
        """Detail a reader may want while looking into a run (what a check found, what was skipped and why);
        the job view hides it unless asked."""
        self._emit("debug", msg, args)

    def info(self, msg: str, *args: Any) -> None:
        self._emit("info", msg, args)

    def warning(self, msg: str, *args: Any) -> None:
        self._emit("warning", msg, args)

    def error(self, msg: str, *args: Any) -> None:
        self._emit("error", msg, args)

    def exception(self, msg: str, *args: Any) -> None:
        self._emit("error", msg, args, exc_info=True)


class StepContext:
    # How often should_stop() may ask the database whether a cancel was requested elsewhere.
    STOP_POLL_SECONDS = 1.0

    def __init__(self, runtime: JobRuntime, run: JobRun, kind: JobKind) -> None:
        self.runtime = runtime
        self.run = run
        self.kind = kind
        self.run_id = run.id
        self.payload: dict[str, Any] = dict(run.payload)
        self.subject: str | None = run.subject
        self.actor: Actor = run.actor
        self.attempt: int = run.attempts + 1
        self.step: str | None = run.step
        self.log = RunLog(self)
        self._cancel_seen = False
        self._polled_at = 0.0

    def progress(
        self, done: float, total: float | None = None, unit: str = "items", message: str | None = None
    ) -> None:
        """Report progress within the current step; the API shows the latest report per step.
        Safe to call from a thread."""
        report = progress(done, total, unit)
        text = message or (f"{done:g}/{total:g} {unit}" if total is not None else f"{done:g} {unit}")
        self.runtime.events.add(self.run_id, "info", self.step, text, report)

    async def save(self) -> None:
        """Checkpoint ``payload`` and ``subject`` now, mid-step: a retry or a restart then sees them.
        (Both are saved anyway whenever a step finishes.)"""
        await self.runtime._save(self)

    async def should_stop(self) -> bool:
        """True once the run was cancelled or the worker is shutting down: a long step checks it between
        units of work and returns (or raises ``RunStopped``) at a point where stopping is safe."""
        if self.runtime.stopping or self._cancel_seen or self.runtime._cancel_local(self.run_id):
            return True
        now = time.monotonic()
        if now - self._polled_at >= self.STOP_POLL_SECONDS:
            self._polled_at = now
            self._cancel_seen = await self.runtime._cancel_requested(self.run_id)
        return self._cancel_seen

    async def check_stop(self) -> None:
        """Raise ``RunStopped`` when ``should_stop()``."""
        if await self.should_stop():
            raise RunStopped()

    async def enqueue(
        self, kind: str, subject: str | None = None, payload: dict[str, Any] | None = None, **options: Any
    ) -> Enqueued:
        """Queue a child run: its actor is this run (``job:<id>``, via ``job``) and its parent this run, so
        GET /jobs/{id}/related shows the tree; ``scope`` defaults to this run's. Other options as
        ``JobRuntime.enqueue``."""
        options.setdefault("actor", Actor("job", str(self.run_id), self.kind.name, "job"))
        options.setdefault("scope", self.run.scope)
        return await self.runtime.enqueue(kind, subject, payload, parent_id=self.run_id, **options)

    async def audit(self, action: str, **fields: Any) -> int | None:
        """Record an audit row for something this step did, as the actor that queued the run, with
        ``job_run_id`` set and, unless given, the run's ``scope``. Needs the runtime's ``audit_table``; None
        without it."""
        fields.setdefault("scope", self.run.scope)
        return await self.runtime._audit_step(AuditEntry(action, actor=self.actor, job_run_id=self.run_id, **fields))
