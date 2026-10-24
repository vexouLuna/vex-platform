"""The standard job routes, mounted by each application under its ``/api/v2``.

    GET    /jobs                      ?state=&kind=&subject=&parent=&cursor=&limit=
    GET    /jobs/counts               ?kind=&subject=&since= (runs per state)
    POST   /jobs                      queue a run
    GET    /jobs/{id}
    PATCH  /jobs/{id}                 pause_before, pause_next
    POST   /jobs/{id}/pause|resume|retry|cancel   retry: step, at, pause_before
    GET    /jobs/{id}/events          ?cursor= (tail: pass back next_cursor, it is never null)
    GET    /jobs/{id}/related         the tree of runs it is in (who queued whom)
    GET    /job-kinds

Every change is audited by the runtime in its own transaction (``job.enqueue``, ``job.cancel`` ...).
"""

from __future__ import annotations

from collections.abc import Callable, Container
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request, status
from pydantic import Field

from ..actor import Actor
from ..api.errors import ApiError
from ..api.middleware import request_id
from ..api.models import ApiModel, UtcDatetime
from ..api.pagination import MAX_LIMIT, Page, decode_cursor, encode_cursor, page_of
from .errors import InvalidJob, JobConflict, JobNotFound
from .events import Level
from .run import STATES, JobRun
from .runtime import JobRuntime, kinds_info


class ActorOut(ApiModel):
    kind: str
    id: str | None
    login: str | None
    via: str


class JobOut(ApiModel):
    id: int
    kind: str
    subject: str | None
    scope: str | None
    state: str
    step: str | None
    steps: list[str]
    payload: dict[str, Any]
    attempts: int
    last_error: str | None
    not_before: UtcDatetime | None
    pause_before: list[str] | None
    pause_next: bool
    cancel_requested: bool
    actor: ActorOut
    parent_id: int | None = Field(default=None, description="The run that queued this one, if a run did")
    created_at: UtcDatetime
    updated_at: UtcDatetime
    started_at: UtcDatetime | None
    finished_at: UtcDatetime | None


class RelatedOut(ApiModel):
    root_id: int = Field(description="The tree's root: the furthest run up the parent links")
    items: list[JobOut] = Field(description="The root and every run under it, oldest first; parent_id links them")
    truncated: bool = Field(description="True when the tree had more runs than ``limit``")


class EventOut(ApiModel):
    id: int
    at: UtcDatetime
    level: str
    step: str | None
    message: str
    progress: dict[str, Any] | None


class EventPage(Page[EventOut]):
    # With ``before`` or ``tail``: pass it back as ``before`` for the page of older events; null when there are none.
    prev_cursor: str | None = None


class JobKindOut(ApiModel):
    name: str
    description: str
    steps: list[str]
    pause_before: list[str]
    cancel_mode: str
    max_attempts: int | None


class JobCountsOut(ApiModel):
    counts: dict[str, int] = Field(description="Every state, 0 when none")
    total: int


class EnqueueIn(ApiModel):
    kind: str
    subject: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    step: str | None = None
    pause_before: list[str] | None = None
    paused: bool = False


class UpdateIn(ApiModel):
    pause_before: list[str] | None = Field(default=None, description="Steps to pause before; null: the kind's")
    pause_next: bool | None = None


class ResumeIn(ApiModel):
    once: bool = False


class RetryIn(ApiModel):
    step: str | None = Field(default=None, description="Where to start; default: where it stopped, or the first step")
    at: UtcDatetime | None = Field(default=None, description="Start then instead of now")
    pause_before: list[str] | None = Field(
        default=None, description="Its gates; null: the kind's; absent: as they were"
    )


def default_actor(request: Request) -> Actor:
    actor = getattr(request.state, "actor", None)
    if not isinstance(actor, Actor):
        raise ApiError(401, detail="no authenticated actor on this request")
    return actor


def jobs_router(
    runtime: JobRuntime,
    auth: Any,
    *,
    actor_of: Callable[[Request], Actor] = default_actor,
    enqueue_kinds: Container[str] | None = None,
) -> APIRouter:
    """``auth`` is the dependency that admits a caller (and sets ``request.state.actor``);
    ``enqueue_kinds`` limits which kinds ``POST /jobs`` may queue (None: all)."""
    router = APIRouter(tags=["jobs"], dependencies=[Depends(auth)])
    registry = runtime.registry

    def out(run: JobRun) -> JobOut:
        kind = registry.kinds.get(run.kind)
        return JobOut(
            id=run.id,
            kind=run.kind,
            subject=run.subject,
            scope=run.scope,
            state=run.state,
            step=run.step,
            steps=list(kind.steps) if kind else [],
            payload=run.payload,
            attempts=run.attempts,
            last_error=run.last_error,
            not_before=run.not_before,
            pause_before=run.pause_before,
            pause_next=run.pause_next,
            cancel_requested=run.cancel_requested,
            actor=ActorOut(**run.actor.as_dict()),
            parent_id=run.parent_id,
            created_at=run.created_at,
            updated_at=run.updated_at,
            started_at=run.started_at,
            finished_at=run.finished_at,
        )

    async def call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return await fn(*args, **kwargs)
        except JobNotFound as exc:
            raise ApiError(404, "job_not_found", str(exc)) from None
        except JobConflict as exc:
            raise ApiError(409, "job_conflict", str(exc)) from None
        except InvalidJob as exc:
            raise ApiError(422, "invalid_job", str(exc)) from None

    def who(request: Request) -> dict[str, Any]:
        return {"actor": actor_of(request), "request_id": request_id(request)}

    @router.get("/jobs", response_model=Page[JobOut])
    async def list_jobs(
        state: Annotated[list[str] | None, Query(description=f"One or more of {', '.join(STATES)}")] = None,
        kind: str | None = None,
        subject: str | None = None,
        parent: Annotated[int | None, Query(description="Only the runs this run queued")] = None,
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 50,
    ) -> Page[Any]:
        key = decode_cursor(cursor, size=1)
        runs = await call(
            runtime.list,
            states=state,
            kind=kind,
            subject=subject,
            parent_id=parent,
            before_id=int(key[0]) if key else None,
            limit=limit + 1,
        )
        return page_of([out(r) for r in runs], limit, lambda r: [r.id])

    # Before /jobs/{run_id}, which would take "counts" for an id.
    @router.get("/jobs/counts", response_model=JobCountsOut)
    async def job_counts(
        kind: str | None = None,
        subject: str | None = None,
        since: Annotated[
            UtcDatetime | None, Query(description="Count finished runs only from this time; active ones always")
        ] = None,
    ) -> JobCountsOut:
        counts = await runtime.counts(kind=kind, subject=subject, since=since)
        return JobCountsOut(counts=counts, total=sum(counts.values()))

    @router.post("/jobs", response_model=JobOut, status_code=status.HTTP_201_CREATED)
    async def enqueue(body: EnqueueIn, request: Request) -> JobOut:
        if enqueue_kinds is not None and body.kind not in enqueue_kinds:
            raise ApiError(422, "invalid_job", f"{body.kind!r} cannot be queued through the API")
        result = await call(
            runtime.enqueue,
            body.kind,
            body.subject,
            body.payload,
            step=body.step,
            pause_before=body.pause_before,
            paused=body.paused,
            **who(request),
        )
        return out(result.run)

    @router.get("/jobs/{run_id}", response_model=JobOut)
    async def get_job(run_id: int) -> JobOut:
        return out(await call(runtime.get, run_id))

    @router.patch("/jobs/{run_id}", response_model=JobOut)
    async def update_job(run_id: int, body: UpdateIn, request: Request) -> JobOut:
        changes: dict[str, Any] = {}
        if "pause_before" in body.model_fields_set:
            changes["pause_before"] = body.pause_before
        if body.pause_next is not None:
            changes["pause_next"] = body.pause_next
        return out(await call(runtime.update, run_id, **changes, **who(request)))

    @router.post("/jobs/{run_id}/pause", response_model=JobOut)
    async def pause_job(run_id: int, request: Request) -> JobOut:
        return out(await call(runtime.pause, run_id, **who(request)))

    @router.post("/jobs/{run_id}/resume", response_model=JobOut)
    async def resume_job(run_id: int, request: Request, body: ResumeIn | None = None) -> JobOut:
        return out(await call(runtime.resume, run_id, once=bool(body and body.once), **who(request)))

    @router.post("/jobs/{run_id}/retry", response_model=JobOut)
    async def retry_job(run_id: int, request: Request, body: RetryIn | None = None) -> JobOut:
        body = body or RetryIn()
        gates = {"pause_before": body.pause_before} if "pause_before" in body.model_fields_set else {}
        return out(await call(runtime.retry, run_id, step=body.step, at=body.at, **gates, **who(request)))

    @router.post("/jobs/{run_id}/cancel", response_model=JobOut)
    async def cancel_job(run_id: int, request: Request) -> JobOut:
        return out(await call(runtime.cancel, run_id, **who(request)))

    @router.get("/jobs/{run_id}/events", response_model=EventPage)
    async def job_events(
        run_id: int,
        cursor: str | None = None,
        before: Annotated[str | None, Query(description="A prev_cursor: the page of events just before it")] = None,
        tail: Annotated[bool, Query(description="The run's newest events (then tail with next_cursor)")] = False,
        min_level: Annotated[Level, Query(description="Leave out events below this level")] = "debug",
        limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 200,
    ) -> EventPage:
        await call(runtime.get, run_id)
        if before is not None or tail:
            key = decode_cursor(before, size=1)
            rows = await runtime.events.list(
                run_id, before=int(key[0]) if key else None, newest=True, min_level=min_level, limit=limit + 1
            )
            older = len(rows) > limit
            items = [EventOut(**r) for r in (rows[1:] if older else rows)]
            return EventPage(
                items=items,
                next_cursor=encode_cursor([items[-1].id if items else 0]),
                prev_cursor=encode_cursor([items[0].id]) if older else None,
            )
        key = decode_cursor(cursor, size=1)
        after = int(key[0]) if key else 0
        rows = await runtime.events.list(run_id, after=after, min_level=min_level, limit=limit)
        items = [EventOut(**r) for r in rows]
        # Never null: a client tails the log by passing the last cursor back.
        return EventPage(items=items, next_cursor=encode_cursor([items[-1].id if items else after]))

    @router.get("/jobs/{run_id}/related", response_model=RelatedOut)
    async def related_jobs(run_id: int, limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 200) -> RelatedOut:
        root, runs, truncated = await call(runtime.related, run_id, limit=limit)
        return RelatedOut(root_id=root, items=[out(r) for r in runs], truncated=truncated)

    @router.get("/job-kinds", response_model=list[JobKindOut])
    async def job_kinds() -> list[JobKindOut]:
        return [JobKindOut(**k) for k in kinds_info(registry)]

    return router
