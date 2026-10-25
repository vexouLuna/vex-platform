# vex-platform

Shared jobs, API and audit building blocks for [twitch-archive](https://github.com/vexouLuna/twitch-archive)
and doomtp-bot, and the conventions both follow ([docs/conventions.md](docs/conventions.md)).

| Module | What it gives an application |
|---|---|
| `vex_platform.jobs` | Job kinds made of steps, run by [procrastinate](https://procrastinate.readthedocs.io) in-process: checkpoints, pause gates, retries with backoff, interrupt or cooperative cancel, per-run event log, dedupe, dynamic concurrency. `jobs_router` serves the standard `/jobs` routes. |
| `vex_platform.audit` | One `audit_log` shape, written with the driver the app already uses (psycopg or SQLAlchemy) inside the change's own transaction; `audit_router` serves `GET /audit`; `AuditRefusalsMiddleware` records refused and failed writes. |
| `vex_platform.api` | RFC 9457 problem+json errors, `X-Request-ID`, `{items, next_cursor}` pages with opaque cursors, a Pydantic base model with ISO 8601 UTC timestamps. |
| `vex_platform.actor` | `Actor(kind, id, login, via)`: who did something, shared by requests, audit rows and job runs. |
| `vex_platform.logging` | structlog setup (JSON or console) that also renders stdlib logging, with URL query redaction. |
| `vex_platform.migrations` | The frozen SQL for the jobs schema and the audit table, applied from each app's own Alembic revisions. |

## Install

```toml
# pyproject.toml of the application
dependencies = ["vex-platform @ git+https://github.com/vexouLuna/vex-platform@v0.9.0"]
```

Python 3.13+, Postgres 13+.

## Use

```python
from vex_platform.jobs import JobRuntime, Registry

registry = Registry()


@registry.step("fetch")
async def fetch(ctx):
    ctx.log.info("fetching %s", ctx.subject)
    ctx.progress(1, 2, "parts")


@registry.step("upload")
async def upload(ctx): ...


registry.kind("archive", ["fetch", "upload"], lock=lambda run: run.subject)

runtime = JobRuntime(registry, DSN, audit_table="public.audit_log")
await runtime.open()
await runtime.start()
await runtime.enqueue("archive", subject="vod:123", actor=actor)
```

Migrations, in an Alembic revision of the application:

```python
from vex_platform import migrations


def upgrade():
    migrations.apply(op, migrations.jobs_sql(1, schema="jobs"))
    migrations.apply(op, migrations.audit_sql(1, table="audit_log"))
```

## Develop

```bash
docker compose up -d postgres-test
uv run pytest
uv run ruff check
```

Tests use `VEX_TEST_DSN` (default `postgresql://vex:vex@127.0.0.1:55434/vex_platform_test`).
