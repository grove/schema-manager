"""Apply generated SQL to Postgres via pg-trickle's idempotent API."""

from __future__ import annotations

import re

import asyncpg

from schema_manager.metrics import schema_ddl_operations_total


async def ensure_pgtrickle_extension(conn: asyncpg.Connection) -> None:
    """Create the pg_trickle extension if it doesn't exist."""
    await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")


async def apply_pgtrickle_sql(conn: asyncpg.Connection, sql: str) -> None:
    """Execute the generated pg-trickle SQL (create_or_replace_stream_table calls).

    Uses pg-trickle's idempotent create_or_replace_stream_table():
    - Unchanged query: no-op (OID preserved, no refresh triggered)
    - Compatible change (ADD/DROP column): ALTER TABLE + full refresh (OID preserved)
    - Incompatible change (type change): storage rebuild + full refresh (OID changes)
    """
    async with conn.transaction():
        await conn.execute(sql)
    schema_ddl_operations_total.labels(operation="pgtrickle_apply").inc()


async def set_stream_schedules(
    conn: asyncpg.Connection,
    stream_names: set[str],
    schedule: str = "30s",
) -> None:
    """Set refresh schedules on leaf delta stream tables.

    Only applies to stream tables whose name starts with '_delta_'.
    """
    for name in sorted(stream_names):
        if name.startswith("_delta_"):
            await conn.execute(
                f"SELECT pgtrickle.alter_stream_table('{name}', schedule => '{schedule}')"
            )


async def apply_stub_ddl(conn: asyncpg.Connection, stub_sql: str) -> None:
    """Apply CREATE TABLE IF NOT EXISTS stubs for all sources."""
    async with conn.transaction():
        await conn.execute(stub_sql)
    schema_ddl_operations_total.labels(operation="stub").inc()


async def enforce_stub_ownership(conn: asyncpg.Connection) -> None:
    """Ensure all inout_src_* staging tables are owned by sesam_ingest.

    Run on every reconcile cycle (not gated by schema hash) so that tables
    created before this fix, or by other tools, are always corrected.
    The ingest engine needs ownership to ALTER TABLE for schema drift.
    """
    rows = await conn.fetch(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname = 'public' AND tablename LIKE 'inout_src_%' "
        "  AND tableowner != 'sesam_ingest'"
    )
    for row in rows:
        tbl = row["tablename"]
        await conn.execute(f"ALTER TABLE {tbl} OWNER TO sesam_ingest")  # noqa: S608


async def drop_orphaned_stream_tables(
    conn: asyncpg.Connection, current_names: set[str]
) -> None:
    """Drop stream tables that exist in the DB but are no longer in config."""
    # pgtrickle may not be installed yet on first boot — skip gracefully.
    ext_loaded = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trickle')"
    )
    if not ext_loaded:
        return
    existing = await conn.fetch("SELECT pgt_name FROM pgtrickle.pgt_stream_tables")
    existing_names = {row["pgt_name"] for row in existing}
    orphans = existing_names - current_names
    for name in orphans:
        await conn.execute("SELECT pgtrickle.drop_stream_table($1)", name)
    if orphans:
        schema_ddl_operations_total.labels(operation="orphan_drop").inc()
