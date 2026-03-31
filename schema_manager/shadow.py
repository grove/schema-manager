"""Shadow mode policy, promotion, and shadow_log management."""

from __future__ import annotations

import asyncpg
import structlog

from schema_manager.component_gate import set_desired

log = structlog.get_logger()

SELF_UPGRADE_SQL = """
CREATE TABLE IF NOT EXISTS shadow_log (
    id          BIGSERIAL PRIMARY KEY,
    logged_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    target      TEXT NOT NULL,
    operation   TEXT NOT NULL,      -- 'create' | 'update' | 'delete'
    external_id TEXT NOT NULL,
    payload     JSONB
);
"""


async def self_upgrade(conn: asyncpg.Connection) -> None:
    """Ensure shadow_log table exists."""
    await conn.execute(SELF_UPGRADE_SQL)


async def promote(config_path: str, dsn_override: str | None = None) -> None:
    """Promote writeback from shadow to live (CLI entrypoint)."""
    from schema_manager.config import SchemaManagerConfig

    cfg = SchemaManagerConfig.from_file(config_path)
    dsn = dsn_override or cfg.database_dsn

    conn = await asyncpg.connect(dsn)
    try:
        current = await conn.fetchval(
            "SELECT desired FROM component_state WHERE component = 'writeback'"
        )
        if current != "shadow":
            log.warning("writeback is not in shadow mode, nothing to promote", current=current)
            return
        await set_desired(conn, "writeback", "running")
        log.info("writeback promoted from shadow to live")
    finally:
        await conn.close()


def should_shadow(on_change_policy: str, tier: int) -> bool:
    """Return True if writeback should enter shadow mode for this change tier."""
    if on_change_policy == "never":
        return False
    if on_change_policy == "always":
        return True
    if on_change_policy == "new_targets_only":
        return tier >= 2
    return True  # safe default
