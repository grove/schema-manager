"""Version-gated DDL for schema-manager's own internal tables."""

from __future__ import annotations

import asyncpg

# Increment this when adding new tables or columns to schema-manager's own schema.
CURRENT_VERSION = 1

_MIGRATIONS: dict[int, str] = {
    1: """
        CREATE TABLE IF NOT EXISTS schema_manager_version (
            version    INT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """,
}


async def run(conn: asyncpg.Connection) -> None:
    """Apply any pending schema-manager internal migrations."""
    # Bootstrap: ensure the version table itself exists
    await conn.execute(_MIGRATIONS[1])

    installed = await conn.fetchval(
        "SELECT COALESCE(max(version), 0) FROM schema_manager_version"
    )
    for version in sorted(_MIGRATIONS):
        if version <= installed:
            continue
        await conn.execute(_MIGRATIONS[version])
        await conn.execute(
            "INSERT INTO schema_manager_version (version) VALUES ($1)", version
        )
