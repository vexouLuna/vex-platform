"""Exceptions of the jobs layer: the first three for callers, the rest for step code."""

from __future__ import annotations

import datetime as dt


class JobNotFound(LookupError):
    def __init__(self, run_id: int) -> None:
        super().__init__(f"no job run {run_id}")
        self.run_id = run_id


class JobConflict(Exception):
    """The run is in a state that does not allow the action; the message says why."""


class InvalidJob(ValueError):
    """An unknown job kind or step name."""


class StepError(RuntimeError):
    """An expected failure with a readable message: stored as ``last_error`` without a traceback,
    and retried like any other failure."""


class StepRefused(StepError):
    """A step that must not run for this subject: the run fails at once instead of retrying."""


class StepDeferred(StepError):
    """A step that can't run until ``until`` (a timezone-aware datetime), such as one waiting for a daily
    quota to reset: the run is queued again at the same step to start then, with its attempts back at 0.
    How many times a run may be deferred is the step's own call (keep a count in ``ctx.payload``)."""

    def __init__(self, message: str, until: dt.datetime) -> None:
        super().__init__(message)
        if until.tzinfo is None:
            raise ValueError("StepDeferred needs a timezone-aware datetime")
        self.until = until


class SkipTo(Exception):  # noqa: N818 - an outcome, not an error
    """The rest of the run has nothing to do: it goes on at the later ``step`` (skipping the ones
    between), or succeeds now when ``step`` is None. ``message`` goes to the run's log."""

    def __init__(self, message: str, step: str | None = None) -> None:
        super().__init__(message)
        self.step = step


class RunStopped(Exception):
    """Raised by ``StepContext.check_stop()`` when the run was cancelled or the worker is shutting
    down. The step is not finished: a shutdown re-queues the run at this step, a cancel ends it."""
