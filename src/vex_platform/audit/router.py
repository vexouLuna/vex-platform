"""``GET /audit`` and the middleware that audits refused and failed writes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Annotated, Any

import structlog
from fastapi import APIRouter, Depends, Query, Request
from psycopg import AsyncConnection
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ..actor import Actor
from ..api.errors import ApiError, under
from ..api.models import ApiModel, UtcDatetime
from ..api.pagination import MAX_LIMIT, Page, decode_cursor, page_of
from . import psycopg as audit_pg
from .model import AuditEntry

log = structlog.get_logger(__name__)

Connect = Callable[[], AbstractAsyncContextManager[AsyncConnection[Any]]]


class AuditOut(ApiModel):
    id: int
    at: UtcDatetime
    actor_kind: str
    actor_id: str | None
    actor_login: str | None
    via: str
    action: str
    target: str | None
    scope: str | None
    scope_name: str | None = None
    outcome: str
    before: Any = None
    after: Any = None
    detail: Any = None
    request_id: str | None
    job_run_id: int | None


def audit_router(
    connect: Connect,
    auth: Any,
    *,
    table: str = "audit_log",
    visible_scopes: Callable[[Request], Awaitable[Sequence[str] | None]] | None = None,
    find_actor: Callable[[Request, str], Awaitable[tuple[str, str] | None]] | None = None,
    labels: Callable[[Request, list[dict[str, Any]]], Awaitable[None]] | None = None,
) -> APIRouter:
    """``connect()`` yields a psycopg connection (``pool.connection``); ``auth`` is the dependency that
    admits a caller (and sets ``request.state.actor``).

    ``visible_scopes(request)`` may limit a caller to some channels (None: all); the caller's own rows
    stay visible outside them. ``find_actor(request, login)`` turns ``?actor=<login>`` into an
    ``(actor_kind, actor_id)``, so rows written before the login was known match too; without it, or
    when it finds nobody, the login matches ``actor_login``. ``labels(request, rows)`` may fill rows in
    place before they are served: a missing ``actor_login``, and ``scope_name``."""
    router = APIRouter(tags=["audit"], dependencies=[Depends(auth)])

    @router.get("/audit", response_model=Page[AuditOut])
    async def list_audit(
        request: Request,
        action: Annotated[str | None, Query(description="Exact, or ending in '.' for a prefix")] = None,
        target: Annotated[str | None, Query(description="Exact, or ending in ':' for a type")] = None,
        scope: str | None = None,
        actor_kind: str | None = None,
        actor_id: str | None = None,
        actor: Annotated[str | None, Query(min_length=1, description="'me', or a login")] = None,
        outcome: str | None = None,
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 50,
    ) -> Page[Any]:
        key = decode_cursor(cursor, size=1, ints=True)
        caller = getattr(request.state, "actor", None)
        me = (caller.kind, caller.id) if isinstance(caller, Actor) and caller.id else None
        scopes = await visible_scopes(request) if visible_scopes else None
        by: tuple[tuple[str, str] | None, str] | None = None
        if actor is not None and actor.lower() == "me":
            if me is None:
                raise ApiError(400, "invalid", "actor=me needs an identified caller")
            actor_kind, actor_id = me
        elif actor is not None:
            by = (await find_actor(request, actor) if find_actor else None, actor)
        async with connect() as conn:
            rows = await audit_pg.read(
                conn,
                table=table,
                limit=limit + 1,
                before_id=int(key[0]) if key else None,
                actor_kind=actor_kind,
                actor_id=actor_id,
                action=action,
                target=target,
                scope=scope,
                scopes=scopes,
                outcome=outcome,
                own=me,
                actor=by,
            )
        if labels is not None:
            await labels(request, rows)
        return page_of([AuditOut(**r) for r in rows], limit, lambda r: [r.id])

    return router


Writer = Callable[[AuditEntry], Awaitable[Any]]
_READS = {"GET", "HEAD", "OPTIONS"}


class AuditRefusalsMiddleware:
    """Records ``request.denied`` (401/403) and ``request.failed`` (5xx, or an exception) for write
    requests under ``prefix`` whose caller was identified (``request.state.actor``). Successful
    changes are audited by the code that makes them, in their own transaction — not here.

    ``write(entry)`` stores the row on a connection of its own. A failure to write is logged, never
    raised: auditing a refusal must not change the response.
    """

    def __init__(self, app: ASGIApp, write: Writer, prefix: str = "/api/v2") -> None:
        self.app = app
        self.write = write
        self.prefix = prefix

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] in _READS or not under(self.prefix, scope["path"]):
            await self.app(scope, receive, send)
            return
        status = 500

        async def capture(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, capture)
        except Exception:
            await self._record(scope, 500)
            raise
        if status in (401, 403) or status >= 500:
            await self._record(scope, status)

    async def _record(self, scope: Scope, status: int) -> None:
        state = scope.get("state") or {}
        actor = state.get("actor")
        if not isinstance(actor, Actor):
            return
        denied = status in (401, 403)
        entry = AuditEntry(
            action="request.denied" if denied else "request.failed",
            actor=actor,
            outcome="denied" if denied else "failed",
            detail={"method": scope["method"], "path": scope["path"], "status": status},
            request_id=state.get("request_id"),
        )
        try:
            await self.write(entry)
        except Exception:
            log.exception("audit.refusal_write_failed", action=entry.action, status=status)
