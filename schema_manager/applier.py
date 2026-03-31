"""Apply generated SQL to Postgres via pg-trickle's idempotent API."""

from __future__ import annotations

import asyncpg


async def apply_pgtrickle_sql(conn: asyncpg.Connection, sql: str) -> None:
    """Execute the generated pg-trickle SQL (create_or_replace_stream_table calls).

    Uses pg-trickle's idempotent create_or_replace_stream_table():
    - Unchanged query: no-op (OID preserved, no refresh triggered)
    - Compatible change (ADD/DROP column): ALTER TABLE + full refresh (OID preserved)
    - Incompatible change (type change): storage rebuild + full refresh (OID changes)
    """
    async with conn.transaction():
        await conn.execute(sql)


async def apply_stub_ddl(conn: asyncpg.Connection, stub_sql: str) -> None:
    """Apply CREATE TABLE IF NOT EXISTS stubs for all sources."""
    async with conn.transaction():
        await conn.execute(stub_sql)


async def drop_orphaned_stream_tables(
    conn: asyncpg.Connection, current_names: set[str]
) -> None:
    """Drop stream tables that exist in the DB but are no longer in config."""
    existing = await conn.fetch(
        "SELECT pgt_name FROM pgt_stream_tables"
    )
    existing_names = {row["pgt_name"] for row in existing}
    orphans = existing_names - current_names
    for name in orphans:
        await conn.execute(
            "SELECT pgtrickle.drop_stream_table($1)", name
        )
