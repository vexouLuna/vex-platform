from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any

import httpx
import psycopg
import pytest
import structlog
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel

from vex_platform.actor import Actor
from vex_platform.api import (
    ApiError,
    Page,
    RequestIdMiddleware,
    decode_cursor,
    encode_cursor,
    install_error_handlers,
    page_of,
)
from vex_platform.audit import AuditEntry
from vex_platform.audit import psycopg as audit_pg
from vex_platform.audit.router import AuditRefusalsMiddleware, audit_router
from vex_platform.jobs import Registry
from vex_platform.jobs.router import jobs_router
from vex_platform.jobs.run import STATES
from vex_platform.logging import configure_logging, redact

from .conftest import AUDIT, DSN, audit_rows, wait_for


def auth(request: Request) -> None:
    user = request.headers.get("x-test-user")
    if not user:
        raise ApiError(401)
    request.state.actor = Actor("user", user, user, "api")
    if user == "guest" and request.method != "GET":
        raise ApiError(403, detail="read only")


@asynccontextmanager
async def connect():
    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        yield conn


async def write_audit(entry: AuditEntry) -> None:
    async with connect() as conn:
        await audit_pg.record(conn, entry, table=AUDIT)


class Body(BaseModel):
    n: int


def make_app(runtime=None) -> FastAPI:
    app = FastAPI()
    install_error_handlers(app)

    @app.get("/api/v2/items")
    async def items(cursor: str | None = None, limit: int = 2) -> Page[int]:
        key = decode_cursor(cursor, size=1)
        start = key[0] + 1 if key else 0
        rows = list(range(start, min(start + limit + 1, 5)))
        return page_of(rows, limit, lambda r: [r])

    @app.post("/api/v2/echo", dependencies=[Depends(auth)])
    async def echo(body: Body) -> Body:
        return body

    @app.get("/api/v2/teapot")
    async def teapot() -> None:
        raise ApiError(418, "teapot", "short and stout", spout=1)

    @app.get("/api/v2/legacy-http")
    async def legacy_http() -> None:
        raise HTTPException(409, "taken")

    @app.post("/api/v2/crash", dependencies=[Depends(auth)])
    async def crash() -> None:
        raise RuntimeError("kaboom")

    @app.get("/v1/thing")
    async def v1_thing() -> None:
        raise HTTPException(404, "no thing")

    app.include_router(audit_router(connect, auth, table=AUDIT), prefix="/api/v2")
    if runtime is not None:
        app.include_router(jobs_router(runtime, auth), prefix="/api/v2")
    app.add_middleware(AuditRefusalsMiddleware, write=write_audit)
    app.add_middleware(RequestIdMiddleware)
    return app


def client(app: FastAPI) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def test_problem_json_and_request_id():
    async with client(make_app()) as c:
        r = await c.get("/api/v2/teapot", headers={"x-request-id": "abc123"})
        assert r.status_code == 418 and r.headers["content-type"] == "application/problem+json"
        assert r.headers["x-request-id"] == "abc123"
        assert r.json() == {
            "type": "about:blank",
            "title": "I'm a Teapot",
            "status": 418,
            "code": "teapot",
            "detail": "short and stout",
            "request_id": "abc123",
            "spout": 1,
        }

        r = await c.get("/api/v2/nope")
        assert (r.status_code, r.json()["code"]) == (404, "not_found")
        assert len(r.headers["x-request-id"]) == 32

        r = await c.get("/api/v2/legacy-http")
        assert (r.json()["code"], r.json()["detail"]) == ("conflict", "taken")

        r = await c.post("/api/v2/echo", json={"n": "x"}, headers={"x-test-user": "vex"})
        body = r.json()
        assert (r.status_code, body["code"]) == (422, "invalid")
        assert body["errors"][0]["loc"] == ["body", "n"]

        r = await c.get("/v1/thing")  # outside the prefix: FastAPI's shape, unchanged
        assert r.json() == {"detail": "no thing"} and r.headers["content-type"] == "application/json"

        r = await c.get("/api/v2/items", headers={"x-request-id": "bad id with spaces"})
        assert r.headers["x-request-id"] != "bad id with spaces"


async def test_cursor_pages():
    assert decode_cursor(encode_cursor([1, "a"])) == [1, "a"]
    async with client(make_app()) as c:
        seen, cursor = [], None
        while True:
            r = await c.get("/api/v2/items", params={"cursor": cursor} if cursor else {})
            page = r.json()
            seen += page["items"]
            cursor = page["next_cursor"]
            if cursor is None:
                break
        assert seen == [0, 1, 2, 3, 4]
        r = await c.get("/api/v2/items", params={"cursor": "!!!"})
        assert (r.status_code, r.json()["code"]) == (400, "bad_cursor")
        r = await c.get("/api/v2/items", params={"cursor": encode_cursor([1, 2])})
        assert r.status_code == 400


@pytest.mark.usefixtures("dsn")
async def test_audit_routes_and_refusals():
    async with client(make_app()) as c:
        assert (await c.post("/api/v2/echo", json={"n": 1})).status_code == 401  # no actor: not recorded
        assert (await c.post("/api/v2/echo", json={"n": 1}, headers={"x-test-user": "guest"})).status_code == 403
        assert (await c.post("/api/v2/crash", headers={"x-test-user": "vex"})).status_code == 500
        assert (await c.post("/api/v2/echo", json={"n": 1}, headers={"x-test-user": "vex"})).status_code == 200

        rows = await audit_rows()
        assert [(r["action"], r["outcome"], r["actor_id"]) for r in rows] == [
            ("request.denied", "denied", "guest"),
            ("request.failed", "failed", "vex"),
        ]
        assert rows[0]["detail"] == {"method": "POST", "path": "/api/v2/echo", "status": 403}

        r = await c.get("/api/v2/audit", params={"limit": 1}, headers={"x-test-user": "vex"})
        page = r.json()
        assert [i["action"] for i in page["items"]] == ["request.failed"]
        assert page["items"][0]["at"].endswith("Z")
        r = await c.get("/api/v2/audit", params={"cursor": page["next_cursor"]}, headers={"x-test-user": "vex"})
        assert [i["action"] for i in r.json()["items"]] == ["request.denied"]
        assert r.json()["next_cursor"] is None
        r = await c.get("/api/v2/audit", params={"outcome": "denied"}, headers={"x-test-user": "vex"})
        assert len(r.json()["items"]) == 1


@pytest.mark.usefixtures("dsn")
async def test_audit_scopes_own_rows_actor_and_labels():
    async with connect() as conn:
        for entry in [
            AuditEntry("vod.update", Actor("user", "1", "vex", "web"), scope="a"),
            AuditEntry("vod.hide", Actor("user", "2", None, "web"), scope="b"),  # written before its login
            AuditEntry("cc.create", Actor("user", "mod", "mod", "chat"), scope="b"),
        ]:
            await audit_pg.record(conn, entry, table=AUDIT)
        await conn.commit()

    async def visible(request: Request) -> list[str] | None:
        return None if request.state.actor.id == "vex" else ["a"]

    async def find_actor(request: Request, login: str) -> tuple[str, str] | None:
        return ("user", "2") if login == "two" else None

    async def labels(request: Request, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            row["scope_name"] = {"a": "Alpha", "b": "Beta"}.get(row["scope"])
            row["actor_login"] = row["actor_login"] or ("two" if row["actor_id"] == "2" else None)

    app = FastAPI()
    install_error_handlers(app)
    app.include_router(
        audit_router(connect, auth, table=AUDIT, visible_scopes=visible, find_actor=find_actor, labels=labels),
        prefix="/api/v2",
    )

    async def actions(user: str, **params) -> list[str]:
        r = await c.get("/api/v2/audit", params=params, headers={"x-test-user": user})
        assert r.status_code == 200, r.text
        return [i["action"] for i in r.json()["items"]]

    async with client(app) as c:
        assert await actions("vex") == ["cc.create", "vod.hide", "vod.update"]
        assert await actions("mod") == ["cc.create", "vod.update"]  # scope a, and its own row in b
        assert await actions("guest") == ["vod.update"]
        assert await actions("mod", actor="me") == ["cc.create"]
        assert await actions("vex", actor="two") == ["vod.hide"]  # found by id: its row has no login
        assert await actions("vex", actor="VEX") == ["vod.update"]  # nobody found: by actor_login
        assert await actions("guest", actor="mod") == []  # still limited to scope a
        r = await c.get("/api/v2/audit", params={"actor": "two"}, headers={"x-test-user": "vex"})
        [row] = r.json()["items"]
        assert (row["actor_login"], row["scope_name"]) == ("two", "Beta")

    async def anonymous(request: Request) -> None:
        pass

    bare = FastAPI()
    install_error_handlers(bare)
    bare.include_router(audit_router(connect, anonymous, table=AUDIT), prefix="/api/v2")
    async with client(bare) as c:
        r = await c.get("/api/v2/audit", params={"actor": "me"})
        assert (r.status_code, r.json()["code"]) == (400, "invalid")
        assert len((await c.get("/api/v2/audit", params={"actor": "vex"})).json()["items"]) == 1


@pytest.mark.usefixtures("dsn")
async def test_jobs_routes(make_runtime):
    registry = Registry()
    gate = asyncio.Event()

    @registry.step()
    async def one(ctx):
        ctx.progress(50, 100, "percent")

    @registry.step()
    async def two(ctx):
        await gate.wait()

    registry.kind("demo", ["one", "two"], description="Demo", pause_before=("two",))
    runtime = await make_runtime(registry)
    vex = {"x-test-user": "vex"}
    async with client(make_app(runtime)) as c:
        r = await c.get("/api/v2/job-kinds", headers=vex)
        assert r.json()[0]["steps"] == ["one", "two"]

        r = await c.post("/api/v2/jobs", json={"kind": "demo", "subject": "vod:1"}, headers=vex)
        assert r.status_code == 201, r.text
        job = r.json()
        assert (job["state"], job["actor"]["login"], job["steps"]) == ("queued", "vex", ["one", "two"])
        await wait_for(runtime, job["id"], ["paused"])

        r = await c.get(f"/api/v2/jobs/{job['id']}/events", headers=vex)
        events = r.json()
        assert any(e["progress"] == {"done": 50, "total": 100, "unit": "percent"} for e in events["items"])
        r = await c.get(f"/api/v2/jobs/{job['id']}/events", params={"cursor": events["next_cursor"]}, headers=vex)
        assert r.json()["items"] == [] and r.json()["next_cursor"] == events["next_cursor"]

        r = await c.patch(f"/api/v2/jobs/{job['id']}", json={"pause_before": None}, headers=vex)
        assert r.json()["pause_before"] is None
        r = await c.post(f"/api/v2/jobs/{job['id']}/resume", json={"once": False}, headers=vex)
        assert r.json()["state"] == "queued"
        await wait_for(runtime, job["id"], ["running"])
        r = await c.post(f"/api/v2/jobs/{job['id']}/cancel", headers=vex)
        assert r.json()["state"] == "cancelled"
        r = await c.post(f"/api/v2/jobs/{job['id']}/pause", headers=vex)
        assert (r.status_code, r.json()["code"]) == (409, "job_conflict")
        r = await c.post(f"/api/v2/jobs/{job['id']}/retry", headers=vex)
        assert r.json()["state"] == "queued"
        gate.set()
        await wait_for(runtime, job["id"], ["succeeded"])

        r = await c.get("/api/v2/jobs", params={"state": ["succeeded"], "kind": "demo"}, headers=vex)
        assert [j["id"] for j in r.json()["items"]] == [job["id"]]
        r = await c.get("/api/v2/jobs/counts", params={"kind": "demo"}, headers=vex)
        assert r.json() == {"counts": {**dict.fromkeys(STATES, 0), "succeeded": 1}, "total": 1}
        r = await c.get("/api/v2/jobs/counts", params={"since": "2999-01-01T00:00:00Z"}, headers=vex)
        assert r.json()["total"] == 0
        r = await c.get("/api/v2/jobs/counts", params={"subject": "vod:2"}, headers=vex)
        assert r.json()["total"] == 0
        assert (await c.get("/api/v2/jobs/999", headers=vex)).json()["code"] == "job_not_found"
        r = await c.post("/api/v2/jobs", json={"kind": "nope"}, headers=vex)
        assert (r.status_code, r.json()["code"]) == (422, "invalid_job")

        # A succeeded run, again later from its first step, with no gates.
        at = "2099-01-01T00:00:00Z"
        r = await c.post(f"/api/v2/jobs/{job['id']}/retry", json={"at": at, "pause_before": []}, headers=vex)
        assert (r.json()["state"], r.json()["step"], r.json()["not_before"]) == ("queued", "one", at)
        assert r.json()["pause_before"] == []

    actions = [r["action"] for r in await audit_rows("job.")]
    assert actions == ["job.enqueue", "job.update", "job.resume", "job.cancel", "job.retry", "job.retry"]


@pytest.mark.usefixtures("dsn")
async def test_related_jobs_routes(make_runtime):
    registry = Registry()

    @registry.step()
    async def fan_out(ctx):
        await ctx.enqueue("child", subject="vod:1")

    @registry.step()
    async def leaf(ctx):
        pass

    registry.kind("parent", ["fan_out"])
    registry.kind("child", ["leaf"])
    runtime = await make_runtime(registry)
    vex = {"x-test-user": "vex"}
    async with client(make_app(runtime)) as c:
        root = (await c.post("/api/v2/jobs", json={"kind": "parent"}, headers=vex)).json()
        assert root["parent_id"] is None
        await wait_for(runtime, root["id"], ["succeeded"])
        r = await c.get("/api/v2/jobs", params={"parent": root["id"]}, headers=vex)
        [child] = r.json()["items"]
        assert (child["kind"], child["parent_id"], child["actor"]["kind"]) == ("child", root["id"], "job")

        r = await c.get(f"/api/v2/jobs/{child['id']}/related", headers=vex)
        body = r.json()
        assert (body["root_id"], body["truncated"]) == (root["id"], False)
        assert [j["id"] for j in body["items"]] == [root["id"], child["id"]]
        r = await c.get(f"/api/v2/jobs/{root['id']}/related", params={"limit": 1}, headers=vex)
        assert len(r.json()["items"]) == 1 and r.json()["truncated"]
        assert (await c.get("/api/v2/jobs/999999/related", headers=vex)).json()["code"] == "job_not_found"


def test_logging_json(capsys):
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    configure_logging("INFO", "json")
    try:
        structlog.get_logger("t").info("jobs.thing", run_id=3)
        logging.getLogger("uvicorn.access").info("GET %s", "/cb?code=secret&x=1")
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)
        structlog.reset_defaults()
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[0]["event"] == "jobs.thing" and lines[0]["run_id"] == 3 and lines[0]["level"] == "info"
    assert "secret" not in lines[1]["event"] and "[redacted]" in lines[1]["event"]
    assert redact("?token=abc") == "?token=[redacted]"
