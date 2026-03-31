"""Manage the component_state table and advisory lock barrier.

Advisory lock keys (session-scoped — use dedicated connections):
  0x5E5A0000  leader lock   (schema-manager singleton)
  0x5E5A0001  ingest barrier
  0x5E5A0002  writeback barrier
"""

from __future__ import annotations

import asyncpg

LEADER_LOCK_KEY    = 0x5E5A_0000
INGEST_LOCK_KEY    = 0x5E5A_0001
WRITEBACK_LOCK_KEY = 0x5E5A_0002

SELF_UPGRADE_SQL = """
CREATE TABLE IF NOT EXISTS component_state (
    component      TEXT PRIMARY KEY,
    desired        TEXT NOT NULL DEFAULT 'stopped',
    schema_version TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS migration_state (
    id             SERIAL PRIMARY KEY,
    schema_hash    TEXT NOT NULL,
    applied_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_by     TEXT NOT NULL DEFAULT current_user,
    tier           INT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'completed'
);
"""


async def self_upgrade(conn: asyncpg.Connection) -> None:
    """Ensure schema-manager's own tables exist (idempotent)."""
    await conn.execute(SELF_UPGRADE_SQL)


async def try_become_leader(conn: asyncpg.Connection) -> bool:
    """Non-blocking attempt to acquire the leader advisory lock.

    Returns True if this instance is now the leader.
    The caller must hold `conn` open for the process lifetime.
    """
    return await conn.fetchval(
        "SELECT pg_try_advisory_lock($1)", LEADER_LOCK_KEY
    )


async def set_desired(conn: asyncpg.Connection, component: str, state: str) -> None:
    await conn.execute(
        "INSERT INTO component_state (component, desired, updated_at) "
        "VALUES ($1, $2, now()) "
        "ON CONFLICT (component) DO UPDATE "
        "SET desired = $2, updated_at = now()",
        component, state,
    )


async def set_schema_version(
    conn: asyncpg.Connection, component: str, schema_hash: str
) -> None:
    await conn.execute(
        "UPDATE component_state SET schema_version = $2, updated_at = now() "
        "WHERE component = $1",
        component, schema_hash,
    )


async def get_schema_version(conn: asyncpg.Connection, component: str) -> str | None:
    return await conn.fetchval(
        "SELECT schema_version FROM component_state WHERE component = $1", component
    )


async def stop_all(conn: asyncpg.Connection) -> None:
    for component in ("ingest", "writeback"):
        await set_desired(conn, component, "stopped")


async def acquire_migration_locks(conn: asyncpg.Connection, tier: int) -> None:
    """Acquire exclusive advisory locks for the affected components.

    Blocks until all shared locks (held by running replicas) are released.
    Call stop_all() first so replicas will finish their current cycle and stop
    acquiring new shared locks.
    """
    if tier >= 2:
        await conn.execute("SELECT pg_advisory_lock($1)", INGEST_LOCK_KEY)
    await conn.execute("SELECT pg_advisory_lock($1)", WRITEBACK_LOCK_KEY)


async def release_migration_locks(conn: asyncpg.Connection, tier: int) -> None:
    await conn.execute("SELECT pg_advisory_unlock($1)", WRITEBACK_LOCK_KEY)
    if tier >= 2:
        await conn.execute("SELECT pg_advisory_unlock($1)", INGEST_LOCK_KEY)
