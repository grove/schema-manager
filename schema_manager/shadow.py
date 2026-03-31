"""Shadow mode policy, promotion, auto-promotion, and shadow_log management."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

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


def parse_duration(value: str | None) -> timedelta | None:
    """Parse a duration string like '24h', '30m', '7d' into a timedelta."""
    if not value:
        return None
    value = value.strip().lower()
    if value.endswith("d"):
        return timedelta(days=int(value[:-1]))
    if value.endswith("h"):
        return timedelta(hours=int(value[:-1]))
    if value.endswith("m"):
        return timedelta(minutes=int(value[:-1]))
    if value.endswith("s"):
        return timedelta(seconds=int(value[:-1]))
    return None


async def check_auto_promotion(conn: asyncpg.Connection, auto_promote_after: str | None) -> bool:
    """Check if auto-promotion conditions are met and promote if so.

    Returns True if promotion was performed.

    Gates:
    1. auto_promote_after is configured and the duration has elapsed since
       writeback entered shadow mode (approximated by component_state.updated_at).
    2. pgtrickle.quick_health status is OK with stale_tables = 0.
    3. No stream tables have consecutive_errors > 0.
    4. shadow_log is not growing excessively (last-minute row count ≤ previous minute).
    """
    duration = parse_duration(auto_promote_after)
    if duration is None:
        return False

    # Is writeback in shadow mode?
    row = await conn.fetchrow(
        "SELECT desired, updated_at FROM component_state WHERE component = 'writeback'"
    )
    if row is None or row["desired"] != "shadow":
        return False

    shadow_start = row["updated_at"]
    if shadow_start.tzinfo is None:
        shadow_start = shadow_start.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    if now - shadow_start < duration:
        return False  # soak period not elapsed

    # Gate 1: pg-trickle healthy
    try:
        health = await conn.fetchrow("SELECT status, stale_tables FROM pgtrickle.quick_health")
        if health["status"] != "OK" or health["stale_tables"] > 0:
            log.debug("auto-promote blocked: pg-trickle not ready", status=health["status"])
            return False
    except asyncpg.UndefinedTableError:
        # pg-trickle not installed yet — skip gate
        pass

    # Gate 2: no consecutive errors in stream tables
    try:
        error_count = await conn.fetchval(
            "SELECT count(*) FROM pgtrickle.pg_stat_stream_tables "
            "WHERE consecutive_errors > 0"
        )
        if error_count > 0:
            log.debug("auto-promote blocked: stream tables have errors", error_count=error_count)
            return False
    except asyncpg.UndefinedTableError:
        pass

    # Gate 3: shadow_log not growing (last 2 min vs previous 2 min)
    try:
        recent = await conn.fetchval(
            "SELECT count(*) FROM shadow_log WHERE logged_at > now() - interval '2 minutes'"
        )
        previous = await conn.fetchval(
            "SELECT count(*) FROM shadow_log "
            "WHERE logged_at > now() - interval '4 minutes' "
            "AND logged_at <= now() - interval '2 minutes'"
        )
        if recent > 0 and previous > 0 and recent > previous * 2:
            log.debug("auto-promote blocked: shadow_log growing", recent=recent, previous=previous)
            return False
    except asyncpg.UndefinedTableError:
        pass

    # All gates passed — promote
    await set_desired(conn, "writeback", "running")
    log.info("auto-promoted writeback from shadow to running",
             soak_duration=str(duration), shadow_start=str(shadow_start))
    return True
