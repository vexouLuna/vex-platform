# Wire conventions for JSON APIs

Synced from [vexouLuna/conventions](https://github.com/vexouLuna/conventions). These are the rules every new
JSON route follows, in any repo. They are written down from vex-platform's `/api/v2` (`vex_platform.api`),
which already implements them for Python services: use it there rather than rebuilding it.

## The rules

- **Fields are snake_case**, and IDs are integers.
  `{"vod_id": 123, "user_login": "someone"}`, never `{"vodId": 123}`.
- **Timestamps are ISO 8601 in UTC with a `Z` suffix.** Never epoch numbers, never an offset like `+02:00`.
  `"created_at": "2026-10-29T14:03:12.5Z"` (`UtcDatetime` in `models.py`).
- **A number with a unit names the unit in its field name.** A bare `duration` or `size` is not allowed.
  `"duration_seconds": 5400.0`, `"reply_hold_ms": 800`, `"size_bytes": 1048576`.
- **A request body with an unknown field is a 422**, not silently ignored (`ApiModel`, `extra="forbid"`).
  `PATCH {"titel": "x"}` answers 422 with code `invalid`.
- **A list is `{items, next_cursor}`**, paged with `?cursor=&limit=` (default 50, maximum 500). The cursor
  is opaque, `next_cursor` is null on the last page, and a cursor the API didn't issue is a 400 with code
  `bad_cursor` (`pagination.py`). No `total`, no offsets.
  `GET /api/v2/jobs?limit=2` → `{"items": [{…}, {…}], "next_cursor": "WzEyXQ"}`, then
  `GET /api/v2/jobs?limit=2&cursor=WzEyXQ`.
  - The one exception is a log a client tails: its `next_cursor` is never null, so the client keeps polling.
- **Every error is one shape:** RFC 9457 problem details, as `application/problem+json` (`errors.py`).
  `code` is a stable snake_case string a client may branch on; `detail` is for people; `request_id` is
  the response's `X-Request-ID`.

  ```json
  {"type": "about:blank", "title": "Conflict", "status": 409, "code": "job_conflict",
   "detail": "run 12 is succeeded", "request_id": "3f2a…"}
  ```

  - A validation failure adds `errors: [{loc, msg, type}]`:
    `{"status": 422, "code": "invalid", "errors": [{"loc": ["body", "title"], "msg": "Field required", "type": "missing"}], …}`.
  - The default codes per status: 400 `bad_request`, 401 `unauthenticated`, 403 `forbidden`, 404
    `not_found`, 405 `method_not_allowed`, 409 `conflict`, 413 `too_large`, 422 `invalid`, 429
    `rate_limited`, 500 `internal`, 503 `unavailable`. A route may use a more specific code.
- **Statuses mean what HTTP says.** 401 without a caller, 403 for a caller who may not, 404 for what
  doesn't exist (or is hidden from this caller), 409 for a state conflict, 422 for a body that doesn't
  validate. A server error is a 5xx, never a 200 with an empty body.
  `POST /api/v2/jobs/12/cancel` on a finished run → 409 `job_conflict`.
- **Every response carries `X-Request-ID`**, kept from the request when it is 1 to 128 characters of
  `[A-Za-z0-9._:-]`, generated otherwise (`middleware.py`).
  `X-Request-ID: 3f2a9c…`
- **One CSRF header: `X-CSRF-Token`.** A write made with the session cookie sends the session's CSRF
  token in it; a write made with a Bearer key needs none, since a key is never sent automatically.
  `PATCH /api/v2/vods/123` with `Cookie: …` and `X-CSRF-Token: <the token from the session endpoint>`.
- **Verbs:** a state change is `POST /things/{id}/<verb>`, a field edit is `PATCH /things/{id}`.
  `POST /api/v2/jobs/12/retry`, `PATCH /api/v2/commands/7`.
- **Version in the path.** New routes go under `/api/v2` (or the service's next version); a breaking
  change to a released route is a new version, not an edit.
  `GET /api/v2/vods`, not a changed `GET /vods`.

## Legacy surfaces

Some routes predate these rules. They keep their shapes until their clients have moved, and **new routes
never copy them**. A route is legacy when it has any of these:

- **Feathers** (twitch-archive's `/vods`, `/games`, `/emotes`): camelCase (`createdAt`), the envelope
  `{total, limit, skip, data}`, `$limit`/`$skip`/`$sort[field]=-1` query syntax, and errors as
  `{name, message, code, className}`.
- **twitch-archive v1 and admin** (`/v1/*`, `/admin/*`): camelCase in most bodies, a bare `cursor` field or no paging at all, and
  errors as `{error: true, msg}`.
- **FastAPI defaults outside `/api/v2`:** errors as `{"detail": "…"}` (or a list of validation errors).
- **Any other spelling of the CSRF header** (`X-Vex-CSRF`), or an error body like `{"error": "…"}`.

A service that still serves legacy routes says which ones in its own docs. Moving a surface onto these
rules is a new version of it, with the old one kept until nothing calls it.
