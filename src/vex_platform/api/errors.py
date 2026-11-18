"""RFC 9457 problem details for every error under the v2 prefix.

Routes raise ``ApiError(status, code, detail)``; ``HTTPException`` and validation errors from FastAPI
are converted too. Paths outside the prefix keep FastAPI's default ``{"detail": ...}`` so the v1
routes answer exactly as before.
"""

from __future__ import annotations

from collections.abc import Iterable
from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exception_handlers import http_exception_handler, request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from .middleware import request_id

PROBLEM_JSON = "application/problem+json"

# Default ``code`` per status, for errors raised without one (HTTPException, framework 404s).
_CODES = {
    400: "bad_request",
    401: "unauthenticated",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    409: "conflict",
    413: "too_large",
    422: "invalid",
    429: "rate_limited",
    500: "internal",
    503: "unavailable",
}


class ApiError(Exception):
    """``code`` is a stable snake_case identifier clients may branch on; ``detail`` is for people."""

    def __init__(
        self,
        status: int,
        code: str | None = None,
        detail: str | None = None,
        *,
        headers: dict[str, str] | None = None,
        **extra: Any,
    ) -> None:
        self.status = status
        self.code = code or _CODES.get(status, "error")
        self.detail = detail
        self.headers = headers
        self.extra = extra
        super().__init__(detail or self.code)


def file_in_use(kind: str, file_id: str, used_by: Iterable[tuple[str, str]], detail: str | None = None) -> ApiError:
    """409 ``file_in_use``: the stored file ``{kind, id}`` can't be deleted while ``used_by`` (``(kind, id)``
    pairs, e.g. ``("vod", "123")``) still uses it. One shape for any file kind, so clients handle it once::

        {"code": "file_in_use", "detail": ..., "file": {"kind": "song", "id": "song-1"},
         "used_by": [{"kind": "vod", "id": "123"}]}
    """
    users = [{"kind": k, "id": i} for k, i in used_by]
    default = f"the {kind} is still used by {', '.join(f'{u["kind"]} {u["id"]}' for u in users)}"
    return ApiError(409, "file_in_use", detail or default, file={"kind": kind, "id": file_id}, used_by=users)


def _title(status: int) -> str:
    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return "Error"


def problem(
    status: int,
    code: str | None = None,
    detail: str | None = None,
    *,
    request: Request | None = None,
    headers: dict[str, str] | None = None,
    **extra: Any,
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": "about:blank",
        "title": _title(status),
        "status": status,
        "code": code or _CODES.get(status, "error"),
    }
    if detail:
        body["detail"] = detail
    if request is not None and (rid := request_id(request)):
        body["request_id"] = rid
    body.update(extra)
    return JSONResponse(body, status_code=status, headers=headers, media_type=PROBLEM_JSON)


def under(prefix: str, path: str) -> bool:
    prefix = prefix.rstrip("/")
    return path == prefix or path.startswith(prefix + "/")


def install_error_handlers(app: FastAPI, prefix: str = "/api/v2") -> None:
    """Problem details for paths under ``prefix``; FastAPI's defaults everywhere else.

    Replaces any handler the app had for ``HTTPException``/``RequestValidationError`` outside the
    prefix with FastAPI's default, so install it before an app's own handlers for those, if it has any.
    """

    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError) -> JSONResponse:
        if not under(prefix, request.url.path):
            return JSONResponse({"detail": exc.detail or exc.code}, status_code=exc.status, headers=exc.headers)
        return problem(exc.status, exc.code, exc.detail, request=request, headers=exc.headers, **exc.extra)

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> Response:
        if not under(prefix, request.url.path):
            return await http_exception_handler(request, exc)
        detail = exc.detail if isinstance(exc.detail, str) else None
        return problem(exc.status_code, None, detail, request=request, headers=getattr(exc, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def _invalid(request: Request, exc: RequestValidationError) -> Response:
        if not under(prefix, request.url.path):
            return await request_validation_exception_handler(request, exc)
        errors = [
            {"loc": list(e.get("loc", ())), "msg": e.get("msg", ""), "type": e.get("type", "")} for e in exc.errors()
        ]
        return problem(422, "invalid", "The request did not validate.", request=request, errors=errors)
