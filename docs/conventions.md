# Conventions

The shared language of twitch-archive and doomtp-bot: the words both use, the shape of their APIs,
audit rows, job runs and logs. `vex-platform` implements these rules. When code and this document
disagree, fix one of them in the same change.

## Glossary

| Term | Meaning |
|---|---|
| **job kind** | A named, ordered list of steps plus its policy: lock, retries, cancel mode, default pause gates, queue, priority. It is registered with `Registry.kind`. |
| **job run** | One execution of a kind: a `job_runs` row. It is the record the API shows and the thing people act on. |
| **step** | One `async def step(ctx)` function. A run checkpoints after each step and resumes at the first unfinished one. |
| **attempt** | One try at the current step. It resets to 0 when a step succeeds and when someone retries the run. |
| **pause gate** | A step a run stops before, in state `paused`, until someone resumes it. |
| **event** | One line in a run's log (`job_run_events`): a message, a progress report, or both. |
| **progress** | `{done, total, unit}`, where `unit` is one of `items`, `parts`, `bytes`, `percent` or `seconds`. |
| **subject** | What a run works on, as a target (`vod:123`). |
| **actor** | Who did something: `Actor(kind, id, login, via)`. |
| **actor kind** | `user`, `api_key`, `system`, `job` or `anonymous`. |
| **via** | The surface an action came through: `api`, `web`, `chat`, `cli`, `job` or `system`. |
| **action** | What was done, written dotted as `noun.verb`. |
| **target** | What it was done to, written `type:id`. |
| **scope** | The channel a change belongs to (a Twitch user id), or null for a global change. |
| **outcome** | `ok`, `denied` or `failed`. |

## Names

- **Targets** are always `type:id`, with a lowercase type. Examples: `vod:123`, `channel:456`, `command:hello`, `setting:runner_concurrency`, `file:clips/x.mp4`. Build them with `target("vod", 123)`.
- **Actions** are dotted lowercase `noun.verb`, and nouns may nest. Examples: `vod.update`, `vod.hide`, `vod.chapters.replace`, `cc.create`, `storage.delete`, `setting.update`, `job.cancel`.
  - An HTTP method or a path is never an action; `AuditEntry` refuses one.
  - Actions the package writes itself:
    - `job.enqueue`, `job.merge`, `job.pause`, `job.resume`, `job.retry`, `job.cancel`, `job.update`
    - `request.denied`, `request.failed`
- **Log events** are dotted `area.what_happened`. Examples: `jobs.step_failed`, `audit.refusal_write_failed`, `http.request`.

## API

These rules apply to everything under `/api/v2`. The v1 routes keep their current shapes until their clients have moved.

- **JSON:** field names are snake_case, IDs are integers, and timestamps are ISO 8601 in UTC with a `Z` suffix (`ApiModel`, `UtcDatetime`). Epoch milliseconds are never used.
- **Models:** every request body and response is a Pydantic model, so the OpenAPI spec is complete. The docs are served at `/api/v2/docs` behind admin auth.
- **Lists:** a list returns `{items, next_cursor}`.
  - The query parameters are `?cursor=&limit=`; the default limit is 50 and the maximum 500.
  - Cursors are opaque; `next_cursor` is null on the last page.
  - A cursor the API didn't issue gets a 400 with code `bad_cursor`.
  - The one exception is an event log a client tails (`/jobs/{id}/events`). Its `next_cursor` is never null, so the client keeps polling with it.
- **Errors:** every error is `application/problem+json` (RFC 9457):
  ```json
  {"type": "about:blank", "title": "Conflict", "status": 409, "code": "job_conflict",
   "detail": "run 12 is succeeded", "request_id": "…"}
  ```
  - `code` is a stable snake_case string that clients may branch on; `detail` is written for people.
  - A validation failure is a 422 with code `invalid`, plus `errors: [{loc, msg, type}]`.
  - Routes raise `ApiError(status, code, detail)`. `install_error_handlers(app)` converts FastAPI's own errors under the prefix and leaves every other path as it was.
- **Request IDs:** every response carries `X-Request-ID` (`RequestIdMiddleware`).
  - The incoming header is kept when it is 1 to 128 characters of `[A-Za-z0-9._:-]`; otherwise a new ID is generated.
  - The ID is bound into every log line and stored on audit rows.
- **Auth:** a Bearer API key or the session cookie, and writes made with the cookie need `X-CSRF-Token`. Both repos already work this way.
  - The auth dependency sets `request.state.actor`.
  - A route that acts needs an actor; without one it returns 401.
- **Verbs:** state changes are `POST /things/{id}/<verb>` (`/jobs/12/cancel`), and field edits are `PATCH /things/{id}`.

## Audit

There is one table shape, `audit_log`, created by `migrations.audit_sql(1, table=...)`:

| Column | Notes |
|---|---|
| `id`, `at` | bigserial; `timestamptz`, defaults to `now()` |
| `actor_kind`, `actor_id`, `actor_login`, `via` | from the `Actor` |
| `action` | dotted |
| `target` | `type:id`, or null |
| `scope` | channel id, or null |
| `outcome` | `ok`, `denied` or `failed` |
| `before`, `after`, `detail` | jsonb |
| `request_id`, `job_run_id` | links to the request or job run |

- **Write** audit rows explicitly, inside the transaction that makes the change. Use `vex_platform.audit.psycopg.record(conn, entry)` or `vex_platform.audit.sqlalchemy.record(conn_or_session, entry)`.
  - If the change rolls back, so does its audit row.
  - Middleware never writes the `ok` rows.
- **Record `before` and `after`** for edits, with only the fields that changed when that is practical. Use `detail` for anything else, such as counts, sizes or reasons.
- **Refusals are recorded by `AuditRefusalsMiddleware`:** `request.denied` for 401/403 and `request.failed` for 5xx or an exception, on write requests from an identified caller.
  - These rows go through a connection of their own.
  - A failure to write one is logged and never changes the response.
- **The table is append-only.** Application roles get `SELECT, INSERT` on it and nothing more.
- **Read** it with `GET /api/v2/audit?action=&target=&scope=&actor_kind=&actor_id=&actor=&outcome=&cursor=`.
  - `action=vod.` matches a prefix, and `target=vod:` matches every target of a type.
  - `visible_scopes` can limit a caller to some channels. The caller's own rows stay visible outside them.
  - `actor=me` is the caller's rows. `actor=<login>` is someone's: the app's `find_actor` hook turns the login into an actor kind and id, so rows written without a login match too; otherwise the login matches `actor_login`, in any case.
  - The app's `labels` hook may fill a row's missing `actor_login` and set `scope_name` (a channel's name, say) before the page is served. The table itself never changes.
  - A job run keeps the `scope` it was queued with, and every audit row about it (`job.*`, and a step's `ctx.audit` unless it names another) carries that scope, so a caller limited to a channel sees the whole life of its runs.

## Jobs

`JobRuntime` layers job runs over [procrastinate](https://procrastinate.readthedocs.io), which runs in-process in the application's event loop.

### Tables

- Everything lives in its own Postgres schema, `jobs` by default.
- **Authoritative record:** `job_runs`, together with its log `job_run_events`. Both are created by `migrations.jobs_sql(1, schema=...)`, alongside procrastinate's own tables; `jobs_sql(2)` adds `job_runs.scope` and `jobs_sql(3)` adds `job_runs.parent_id`.
- **Procrastinate's side:** each queued run has one procrastinate job (`vex.run`, holding `run_id`). That job is transport only, and it is deleted once it finishes.

### States

```
queued ──▶ running ──▶ succeeded ──(retry)──▶ queued
  ▲  │        │  │
  │  ▼        │  └──▶ failed ──(retry)──▶ queued
paused ◀──────┘
  │
  └──(cancel from queued/paused/running)──▶ cancelled ──(retry)──▶ queued
```

- **Retry** (`retry(run_id, step=, at=, pause_before=)`, `POST /jobs/{id}/retry`) queues any finished run again with fresh attempts: a failed or cancelled one at `step` or where it stopped, a succeeded one at `step` or its first step. `at` starts it at that time instead of now (a "retry in"). `pause_before` replaces the run's gates (null: the kind's), so a retry can stop before steps it ran straight through last time; a gate passed for the step it starts at pauses it at once. A kind's own gate on that step does not: a retry means go.

- `queued`, `running` and `paused` are **active**; `succeeded`, `failed` and `cancelled` are **finished**.
- A run pauses before a step listed in its own `pause_before`, or, when that is NULL, in its kind's. `Registry.set_pause_before(kind, steps)` changes a kind's list while runs are going (an admin setting, for example). Gates are checked when a run moves on to a step, so the change applies from each run's next step boundary.
- **Scheduled runs:** a run queued with a `not_before` later than the next `reconcile()` (`enqueue(not_before=)`, `retry(at=)`, a step's `ctx.enqueue(not_before=)`) waits with no procrastinate job, so it holds no lock, and the reconcile before its time defers its job. It starts up to `reconcile_interval` late if no worker process is running then.
- A run in `running` goes back to `queued` when the process stops (shutdown, crash, or a stalled worker). The next start resumes it at its checkpoint; `reconcile()` does this at startup and every `reconcile_interval` seconds.
- A procrastinate job left `doing` by a dead worker (no worker row, or no heartbeat for 30 seconds) is **stalled**, and it would hold its lock back from later runs for good. `reconcile()` ends it and queues a fresh job, whether its run was `running` or still `queued`, and also ends stalled `vex.run` jobs whose run has moved on to another job. `pause` and `cancel` end a stalled job too; a live worker's job is left to end by itself.

### Steps

```python
@registry.step("download")
async def download(ctx: StepContext) -> None:
    ctx.log.info("fetching %s", ctx.subject)
    ctx.payload["parts"] = 12
    await ctx.save()  # checkpoint payload mid-step
    ctx.progress(3, 12, "parts")
    if await ctx.should_stop():  # cooperative kinds must poll this
        return
    await ctx.audit("vod.update", target="vod:1", before=..., after=...)
```

- **State:** `ctx.payload` is the run's state across steps and attempts. It is saved after each step, or on demand with `ctx.save()`.
- **Errors:**
  - Raise `StepRefused` for a failure that another attempt won't fix; the run fails at once.
  - Raise `StepDeferred(message, until)` when the step can't run before a known time, such as a daily quota resetting. The run is queued again at the same step with `not_before = until` and its attempts back at 0, and `last_error` holds the message. A run may be deferred any number of times, so a step that should give up keeps its own count in `ctx.payload` and raises `StepRefused` past it. Like a retry backoff, a deferred run keeps its lock while it waits.
  - Raise `SkipTo(message, step=None)` when the rest of the run has nothing to do, such as a VOD with nothing left to upload. The run goes on at the later `step`, skipping the ones between, or succeeds now when `step` is None. It is not an error: `last_error` stays empty and the message goes to the run's log. A step that is not later in the kind fails the run.
  - Any other exception is retried after a backoff of `retry_base_seconds · 2^(attempt-1)`, up to `max_attempts` attempts. The retry resumes at the failed step.
- **Custom context:** `context_factory` lets an application wrap `StepContext` in its own context class, such as twitch-archive's `JobContext`.

### Cancel modes

- **`interrupt`** (the default): cancelling a running run aborts its procrastinate job. The step gets `CancelledError` at its next `await`.
- **`cooperative`**: cancelling only sets `cancel_requested`, and the step stops when `ctx.should_stop()` returns true.
  - Use it when an interrupted step could leave shared state inconsistent. An example is doomtp-bot's backfill, which uses savepoints on a shared connection.
- `cancel(wait=5)` waits up to that many seconds for the run to stop, and returns the run as it stands then.

### Dedupe and locks

- **`queued_key`:** at most one *queued* run per `(kind, queued_key)`.
- **`active_key`:** at most one queued, running or paused run per `(kind, active_key)`.
- **On a duplicate**, `enqueue` either returns the existing run, merges the new payload into it (`on_duplicate="merge"`, audited as `job.merge`), or raises `JobConflict`.
- **`lock`:** `JobKind.lock(run)` returns a string, and runs with the same lock never run at the same time (procrastinate's `lock`).
  - A queued run holds its lock **even while it waits out a retry backoff** (a scheduled run does not, see "Scheduled runs"), so later runs with the same lock wait behind it. Keep lock keys narrow, for example the subject rather than the kind.

### Runs that queue runs

- A step queues a child run with `await ctx.enqueue(kind, subject, payload, **options)`. The child's actor is the parent run (`Actor("job", "<run id>", "<kind>", "job")`), its `parent_id` is the parent's id, and its `scope` defaults to the parent's.
- Plain `enqueue` with a `"job"` actor whose id is a run id sets `parent_id` the same way; `parent_id=` sets it explicitly. A duplicate (see below) keeps the parent it had.
- `GET /jobs?parent=<id>` lists one run's children. `GET /jobs/{id}/related` (`runtime.related`) returns the whole tree the run is in as `{root_id, items, truncated}`: the root and every run under it, oldest first, linked by `parent_id`.

### Enqueuing inside your own transaction

`enqueue(..., conn=conn)`, along with `pause`, `resume`, `retry`, `cancel` and `update`, can each join the caller's psycopg transaction:
- The procrastinate job and the audit row are written in a savepoint on that connection.
- The jobs schema is prepended to `search_path` for the savepoint's duration.
- If the caller rolls back, the run was never queued.

### Concurrency

- `concurrency` is how many runs execute at once, and `set_concurrency(n)` changes it live.
- It can go up to `max_concurrency`, which is the procrastinate worker's own ceiling.
- The worker fetches a job only once a slot is free, so a run waiting for a slot keeps a `todo` job: a crash meanwhile leaves nothing `doing`, and `pause` or `cancel` can drop it.

### Hooks

`runtime.hooks.append(fn)` calls `fn(event, run)` after `started`, `succeeded`, `failed`, `cancelled`, `paused`, `requeued` (stopped by shutdown), `retrying` (a failed attempt that will be retried) and `deferred` (a step raised `StepDeferred`). Use hooks for metrics, such as doomtp-bot's Prometheus counters.

## Logging

- Call `configure_logging(level, "json")` in containers and `"console"` locally.
- Use structlog everywhere: `log = structlog.get_logger(__name__)`, then `log.info("jobs.step_failed", run_id=…, step=…)`.
- Stdlib loggers such as uvicorn are rendered the same way.
- `request_id` is bound for every request; job code binds `run_id` and `step`.
- OAuth codes and tokens in logged URLs are replaced with `[redacted]`.

## Migrations and procrastinate versions

- Applications apply the frozen SQL from their own migrations:
  - Alembic: `migrations.apply(op, migrations.jobs_sql(1))`, and the same for `audit_sql`. Each later revision (`jobs_sql(2)`, ...) goes in a new revision of the application's own. It works over psycopg, psycopg2 and asyncpg, inside the revision's transaction.
  - Plain SQL runners: execute the string as is.
- **Procrastinate is pinned exactly** (`procrastinate==3.10.0`), because `jobs_sql(1)` is its schema as of that version.
  - Upgrading procrastinate means adding its migration files as the next `jobs_sql` revision, applied by a new migration in each application, and releasing a new vex-platform version.
  - Never edit a released revision's SQL.
- **`search_path`:** procrastinate's SQL, including its triggers, uses unqualified table names.
  - The runtime's pool sets `search_path` to the jobs schema.
  - Any other connection that inserts, updates or deletes procrastinate rows must also have the jobs schema on its `search_path`, or the triggers fail with `UndefinedTable`. `JobRuntime.transaction(conn)` handles this for you.
- **Qualify `audit_table`** with its schema (`public.audit_log`) when you pass it to `JobRuntime`, because job actions write their audit rows on the runtime's pool, whose `search_path` is the jobs schema.
- **Migrations are additive.** Both applications deploy on merge and run migrations against the live database, so drops and renames need their own confirmed change once nothing uses the old shape.

## Spike findings (procrastinate 3.10.0)

- The schema can live in its own Postgres schema through the pool's `search_path`. The LISTEN connection reuses the pool's settings.
- `open_async(pool)` accepts an external `AsyncConnectionPool`, and `configure(connection=conn)` and `cancel_job_by_id_async(..., connection=conn)` join a caller's transaction.
- An aborted async task gets `CancelledError`.
- `run_worker_async` runs beside uvicorn in one loop with `install_signal_handlers=False`.
- Changing concurrency by restarting the worker works, but the runtime instead keeps the worker at `max_concurrency` and limits runs itself, so nothing restarts.
- On Windows, psycopg's async connections need `WindowsSelectorEventLoopPolicy`.
