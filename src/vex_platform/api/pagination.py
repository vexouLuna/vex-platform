"""Keyset pagination: ``?cursor=&limit=`` in, ``{items, next_cursor}`` out.

A cursor is opaque to clients: base64url of a small JSON list (the sort key of the last row sent).
``next_cursor`` is null on the last page.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Callable
from typing import Annotated, Any

from fastapi import Query
from pydantic import BaseModel

from .errors import ApiError

DEFAULT_LIMIT = 50
MAX_LIMIT = 500

Cursor = list[Any]


class Page[T](BaseModel):
    items: list[T]
    next_cursor: str | None = None


def encode_cursor(key: Cursor | None) -> str | None:
    if key is None:
        return None
    raw = json.dumps(key, separators=(",", ":"), default=str).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def decode_cursor(value: str | None, size: int | None = None, *, ints: bool = False) -> Cursor | None:
    """``size``: how many key parts the cursor must have; ``ints``: they must all be integers (a 400 otherwise)."""
    if not value:
        return None
    try:
        key = json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
    except (binascii.Error, ValueError) as exc:
        raise ApiError(400, "bad_cursor", "The cursor is not one this API issued.") from exc
    if not isinstance(key, list) or (size is not None and len(key) != size):
        raise ApiError(400, "bad_cursor", "The cursor is not one this API issued.")
    if ints and not all(isinstance(k, int) and not isinstance(k, bool) for k in key):
        raise ApiError(400, "bad_cursor", "The cursor is not one this API issued.")
    return key


def limit_param(default: int = DEFAULT_LIMIT, maximum: int = MAX_LIMIT) -> Any:
    """A ``limit`` query parameter: ``limit: int = limit_param()``."""
    return Query(default, ge=1, le=maximum, description=f"Rows per page (max {maximum}).")


LimitQuery = Annotated[int, Query(ge=1, le=MAX_LIMIT, description=f"Rows per page (max {MAX_LIMIT}).")]


def page_of[T](rows: list[T], limit: int, key: Callable[[T], Cursor]) -> Page[T]:
    """``rows`` fetched with ``LIMIT limit + 1``: the extra row only says there is a next page.
    ``key(row)`` is the cursor for the last row kept."""
    more = len(rows) > limit
    rows = rows[:limit]
    return Page[Any](items=rows, next_cursor=encode_cursor(key(rows[-1])) if more and rows else None)
