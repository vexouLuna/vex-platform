"""Versioned SQL for the tables this package owns, applied by each application's own Alembic revisions.

The SQL is frozen per revision so an Alembic revision always creates the same schema, whichever
vex-platform version is installed when it runs:

    # migrations/versions/0012_vex_platform.py (twitch-archive)
    from vex_platform import migrations

    def upgrade():
        migrations.apply(op, migrations.jobs_sql(1, schema="jobs"))
        migrations.apply(op, migrations.audit_sql(1, table="audit_log"))

``jobs_sql(1)`` is procrastinate 3.10.0's schema plus ``job_runs`` and ``job_run_events``, all in their own
Postgres schema. ``jobs_sql(2)`` adds ``job_runs.scope``, ``jobs_sql(3)`` ``job_runs.parent_id``, ``jobs_sql(4)`` the
``debug`` event level and an index for pruning progress reports. Each later
revision (a procrastinate bump, a new column) is applied by a new Alembic revision in each application, and a
vex-platform release that reads it says so in its notes.
"""

from __future__ import annotations

import re
from importlib import resources
from typing import Any

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")

# Revision -> files, applied in order inside the jobs schema.
JOBS_REVISIONS: dict[int, tuple[str, ...]] = {
    1: ("procrastinate_3.10.0.sql", "jobs_0001.sql"),
    2: ("jobs_0002.sql",),
    3: ("jobs_0003.sql",),
    4: ("jobs_0004.sql",),
}
AUDIT_REVISIONS: dict[int, tuple[str, ...]] = {
    1: ("audit_0001.sql",),
}
JOBS_HEAD = max(JOBS_REVISIONS)
AUDIT_HEAD = max(AUDIT_REVISIONS)


def _read(name: str) -> str:
    return (resources.files(__package__) / "sql" / name).read_text(encoding="utf-8")


def _ident(value: str, what: str) -> str:
    if not _IDENT.match(value):
        raise ValueError(f"{what} {value!r} must be a lowercase SQL identifier")
    return value


def jobs_sql(revision: int, *, schema: str = "jobs") -> str:
    """SQL creating (revision 1) or upgrading the jobs tables in ``schema``.

    It sets ``search_path`` to ``schema`` for its own statements (procrastinate's SQL is unqualified)
    and puts the previous value back at the end, so statements after it in the same migration are
    unaffected.
    """
    schema = _ident(schema, "schema")
    body = "\n\n".join(_read(name) for name in JOBS_REVISIONS[revision])
    create = f"CREATE SCHEMA IF NOT EXISTS {schema};\n" if revision == 1 else ""
    return (
        f"{create}"
        "SELECT set_config('vex.saved_search_path', current_setting('search_path'), false);\n"
        f"SET search_path TO {schema};\n\n"
        f"{body}\n\n"
        "SELECT set_config('search_path', current_setting('vex.saved_search_path'), false);\n"
    )


def audit_sql(revision: int, *, table: str = "audit_log") -> str:
    """SQL creating (revision 1) or upgrading the audit table ``table`` (``schema.name`` or ``name``)."""
    parts = table.split(".")
    if len(parts) > 2:
        raise ValueError(f"table {table!r} must be name or schema.name")
    for part in parts:
        _ident(part, "table")
    sql = "\n\n".join(_read(name) for name in AUDIT_REVISIONS[revision])
    return sql.replace("__TABLE__", table).replace("__NAME__", parts[-1])


def apply(op: Any, sql: str) -> None:
    """Run ``sql`` from an Alembic revision (``op``) or on a SQLAlchemy ``Connection``.

    The SQL holds several statements, colons and percent signs, so it bypasses SQLAlchemy's
    ``text()`` and ``exec_driver_sql`` (which hands the driver a parameter tuple, making psycopg
    parse ``%``): it goes to the driver's cursor without parameters, or to asyncpg's
    ``Connection.execute``, which runs a multi-statement script. Stays in the current transaction.
    """
    bind = op.get_bind() if hasattr(op, "get_bind") else op
    raw = bind.connection
    if bind.dialect.driver == "asyncpg":
        from sqlalchemy.util import await_only

        await_only(raw.driver_connection.execute(sql))
        return
    cursor = raw.dbapi_connection.cursor()
    try:
        cursor.execute(sql)
    finally:
        cursor.close()
