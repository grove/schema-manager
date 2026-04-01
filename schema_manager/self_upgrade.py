"""Version-gated DDL for schema-manager's own internal tables."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import asyncpg

# Increment this when adding new tables or columns to schema-manager's own schema.
CURRENT_VERSION = 8

_MIGRATIONS: dict[int, str] = {
    1: """
        CREATE TABLE IF NOT EXISTS schema_manager_version (
            version    INT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """,
    2: """
        -- Privilege separation: create restricted DB roles for ingest and writeback.
        -- sesam_admin: full DDL owner (schema-manager connects as the bootstrap user).
        -- sesam_ingest: DML only on staging tables + component_state SELECT.
        -- sesam_writeback: DML on sync/writeback tables + component_state SELECT.
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sesam_ingest') THEN
                CREATE ROLE sesam_ingest LOGIN;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sesam_writeback') THEN
                CREATE ROLE sesam_writeback LOGIN;
            END IF;
        END
        $$;

        -- Grant schema usage
        GRANT USAGE ON SCHEMA public TO sesam_ingest, sesam_writeback;

        -- Ingest: INSERT/UPDATE/SELECT on inout_src_* staging tables (granted via default privileges)
        ALTER DEFAULT PRIVILEGES IN SCHEMA public
            GRANT SELECT, INSERT, UPDATE ON TABLES TO sesam_ingest;

        -- Writeback: SELECT on views, DML on sync/writeback tables
        ALTER DEFAULT PRIVILEGES IN SCHEMA public
            GRANT SELECT, INSERT, UPDATE ON TABLES TO sesam_writeback;

        -- Both: SELECT on component_state for gate polling (table may not exist yet
        -- on first run; gate_self_upgrade creates it next. Default privileges cover it.)
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'component_state') THEN
                GRANT SELECT ON component_state TO sesam_ingest, sesam_writeback;
            END IF;
        END
        $$;

        -- Both: advisory lock usage (implicit — no explicit grant needed for advisory locks)

        -- Sequences (for shadow_log, dead letter, etc.)
        ALTER DEFAULT PRIVILEGES IN SCHEMA public
            GRANT USAGE, SELECT ON SEQUENCES TO sesam_ingest, sesam_writeback;
    """,
    3: """
        -- sesam_ingest needs CREATE on the schema so the ingest engine can
        -- run its own CREATE TABLE IF NOT EXISTS safety guard on startup.
        GRANT CREATE ON SCHEMA public TO sesam_ingest;
    """,
    4: """
        -- sesam_ingest queries pgtrickle.pgt_dependencies to check CDC
        -- dependencies before performing schema drift (ALTER TABLE).
        GRANT USAGE ON SCHEMA pgtrickle TO sesam_ingest;
        GRANT SELECT ON ALL TABLES IN SCHEMA pgtrickle TO sesam_ingest;
        ALTER DEFAULT PRIVILEGES IN SCHEMA pgtrickle
            GRANT SELECT ON TABLES TO sesam_ingest;
    """,
    5: """
        -- When sesam_ingest (as staging table owner) runs ALTER TABLE,
        -- pgtrickle's DDL event trigger fires and tries to CREATE OR REPLACE
        -- FUNCTION in pgtrickle_changes. sesam_ingest needs CREATE there.
        GRANT CREATE ON SCHEMA pgtrickle_changes TO sesam_ingest;
        GRANT USAGE ON SCHEMA pgtrickle_changes TO sesam_ingest;
        ALTER DEFAULT PRIVILEGES IN SCHEMA pgtrickle_changes
            GRANT INSERT ON TABLES TO sesam_ingest;
    """,
    6: """
        -- pgtrickle's DDL event trigger runs CREATE OR REPLACE FUNCTION on
        -- functions owned by inandout. PostgreSQL allows this only for the
        -- function's owner OR members of the owning role. Grant sesam_ingest
        -- membership in inandout so it can replace pgtrickle CDC functions
        -- when it ALTERs staging tables for schema drift.
        GRANT inandout TO sesam_ingest;
    """,
    7: """
        -- Simulator gets its own role and schema so its tables (sim_records,
        -- sim_mutations) are fully isolated from inout_* tables in public.
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sesam_simulator') THEN
                CREATE ROLE sesam_simulator LOGIN;
            END IF;
        END
        $$;

        CREATE SCHEMA IF NOT EXISTS simulator AUTHORIZATION sesam_simulator;
        GRANT USAGE, CREATE ON SCHEMA simulator TO sesam_simulator;

        ALTER DEFAULT PRIVILEGES FOR ROLE sesam_simulator IN SCHEMA simulator
            GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO sesam_simulator;
    """,
    8: """
        -- Writeback updates _written flags in source tables. pgtrickle CDC
        -- trigger functions then INSERT into pgtrickle_changes.changes_* using
        -- the caller's privileges. sesam_writeback must have USAGE on the
        -- schema and INSERT on current/future change-buffer tables.
        GRANT USAGE ON SCHEMA pgtrickle_changes TO sesam_writeback;
        GRANT INSERT ON ALL TABLES IN SCHEMA pgtrickle_changes TO sesam_writeback;
        ALTER DEFAULT PRIVILEGES IN SCHEMA pgtrickle_changes
            GRANT INSERT ON TABLES TO sesam_writeback;
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

    # Always sync service-role passwords from env vars (idempotent on every startup).
    await _ensure_role_passwords(conn)


async def _ensure_role_passwords(conn: asyncpg.Connection) -> None:
    """Set passwords on sesam_ingest / sesam_writeback from their DSN env vars.

    Called on every startup so that secret rotations take effect without a
    new migration version.
    """
    for env_var, rolname in [
        ("INGEST_DATABASE_URL", "sesam_ingest"),
        ("WRITEBACK_DATABASE_URL", "sesam_writeback"),
        ("SIMULATOR_DATABASE_URL", "sesam_simulator"),
    ]:
        dsn = os.environ.get(env_var)
        if not dsn:
            continue
        password = urlparse(dsn).password
        if password:
            # ALTER ROLE is DDL — Postgres doesn't allow $1 placeholders in DDL.
            # rolname is a hardcoded literal (not user input).
            # Escape the password via quote_literal() to handle any special chars.
            quoted = await conn.fetchval("SELECT quote_literal($1)", password)
            await conn.execute(
                f"ALTER ROLE {rolname} WITH ENCRYPTED PASSWORD {quoted}"  # noqa: S608
            )
