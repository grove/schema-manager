"""EXPLAIN-based validation of generated view SQL before applying DDL."""

from __future__ import annotations

import re

import asyncpg


class ValidationError(RuntimeError):
    pass


async def validate_views(conn: asyncpg.Connection, pgtrickle_sql: str) -> None:
    """EXPLAIN-validate every SELECT in the generated SQL.

    Resolves all column and table references without executing queries.
    Raises ValidationError with a clear message naming the failing view
    if any reference is broken. No DDL has been applied at this point.
    """
    for view_name, select_sql in _extract_selects(pgtrickle_sql):
        try:
            await conn.execute(f"EXPLAIN {select_sql}")
        except asyncpg.PostgresError as e:
            raise ValidationError(
                f"View '{view_name}' is invalid: {e}\n"
                "Check that mapping.yaml and the current Alembic schema are consistent."
            ) from e


def _extract_selects(sql: str) -> list[tuple[str, str]]:
    """Extract (name, select_sql) pairs from CREATE STREAM TABLE ... AS SELECT ..."""
    results = []
    # Matches: CREATE ... stream_table_name ... AS (SELECT ...)
    # This is a best-effort parser; the full SQL is validated by EXPLAIN.
    pattern = re.compile(
        r"create_or_replace_stream_table\s*\(\s*name\s*=>\s*'([^']+)'.*?query\s*=>\s*\$\$(.+?)\$\$",
        re.IGNORECASE | re.DOTALL,
    )
    for m in pattern.finditer(sql):
        results.append((m.group(1), m.group(2).strip()))
    return results
