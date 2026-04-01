"""Core reconcile loop: compute desired state, diff, apply DDL, manage components."""

from __future__ import annotations

import asyncio
import signal
import time
from pathlib import Path

import asyncpg
import structlog

from schema_manager import alembic_runner, applier, component_gate, self_upgrade, shadow
from schema_manager.config import ConfigInputs, SchemaManagerConfig
from schema_manager.metrics import (
    schema_component_state_changes_total,
    schema_ddl_operations_total,
    schema_leader,
    schema_reconcile_duration_seconds,
    schema_reconcile_total,
    schema_stream_tables,
    schema_validation_errors_total,
)
from schema_manager.osi import OsiError, convert_to_pgtrickle, render_mapping
from schema_manager.stubs import generate_stub_ddl
from schema_manager.validator import ValidationError, validate_views
from schema_manager.watcher import ConfigWatcher

log = structlog.get_logger()


class Reconciler:
    def __init__(
        self, cfg: SchemaManagerConfig, config_path: str, dry_run: bool = False
    ) -> None:
        self.cfg = cfg
        self.config_path = config_path
        self.dry_run = dry_run
        self.is_ready = False
        self._leader_conn: asyncpg.Connection | None = None

    @classmethod
    def from_config_file(cls, path: str, dry_run: bool = False) -> "Reconciler":
        cfg = SchemaManagerConfig.from_file(path)
        return cls(cfg, config_path=path, dry_run=dry_run)

    async def run(self) -> None:
        """Start the service: become leader, reconcile once, then watch for changes."""
        import structlog

        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(
                getattr(__import__("logging"), self.cfg.log_level.upper(), 20)
            ),
        )

        self._leader_conn = await asyncpg.connect(self.cfg.database_dsn)
        if not await component_gate.try_become_leader(self._leader_conn):
            log.error("another schema-manager instance is already running — exiting")
            schema_leader.set(0)
            return

        schema_leader.set(1)
        log.info("acquired leader lock")

        # Bootstrap own tables
        await self_upgrade.run(self._leader_conn)
        await component_gate.self_upgrade(self._leader_conn)
        await shadow.self_upgrade(self._leader_conn)

        # Initial reconcile
        await self.reconcile_once()
        self.is_ready = True

        # Start health server and watch loop concurrently
        from schema_manager.health import build_app, start_server

        app = build_app(self)
        await start_server(app, self.cfg.health_listen)

        watch_dir = self.cfg.mapping_path.parent
        watcher = ConfigWatcher(
            watch_dir=watch_dir,
            poll_interval=self.cfg.poll_interval,
            on_change=self._on_watch_tick,
        )

        _stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _handle_stop(signum: int, frame: object) -> None:
            loop.call_soon_threadsafe(_stop_event.set)

        try:
            signal.signal(signal.SIGTERM, _handle_stop)
            signal.signal(signal.SIGINT, _handle_stop)
        except (OSError, AttributeError):
            pass

        watcher_task = asyncio.create_task(watcher.run())
        stop_task = asyncio.create_task(_stop_event.wait())
        try:
            await asyncio.wait(
                {watcher_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for t in (watcher_task, stop_task):
                if not t.done():
                    t.cancel()
                    try:
                        await t
                    except (asyncio.CancelledError, Exception):
                        pass
            log.info("schema_manager_shutting_down")
            if self._leader_conn is not None and not self._leader_conn.is_closed():
                try:
                    await self._leader_conn.execute(
                        "SELECT pg_advisory_unlock($1)", component_gate.LEADER_LOCK_KEY
                    )
                except Exception:
                    pass
                try:
                    await self._leader_conn.close()
                except Exception:
                    pass
            schema_leader.set(0)
            log.info("schema_manager_stopped")

    async def _on_watch_tick(self) -> None:
        """Called on each watch cycle: reconcile config + check auto-promotion."""
        await self.reconcile_once()
        await self._check_auto_promotion()

    async def _check_auto_promotion(self) -> None:
        """If writeback is in shadow mode and soak gates pass, auto-promote."""
        if not self.cfg.auto_promote_after:
            return
        conn = await asyncpg.connect(self.cfg.database_dsn)
        try:
            promoted = await shadow.check_auto_promotion(
                conn, self.cfg.auto_promote_after
            )
            if promoted:
                self.is_ready = True  # ensure readiness after promotion
        except Exception:
            log.warning("auto-promotion check failed", exc_info=True)
        finally:
            await conn.close()

    async def reconcile_once(self, dry_run: bool | None = None) -> None:
        """Run a single reconcile cycle: compare config hash to DB, apply if changed."""
        if dry_run is None:
            dry_run = self.dry_run

        t0 = time.monotonic()
        conn = await asyncpg.connect(self.cfg.database_dsn)
        try:
            inputs = ConfigInputs.load(self.cfg)
            log.info("config loaded", hash=inputs.schema_hash[:12])

            # Always enforce staging table ownership regardless of schema hash.
            await applier.enforce_stub_ownership(conn)

            current_hash = await component_gate.get_schema_version(conn, "ingest")
            if current_hash == inputs.schema_hash:
                log.info("schema up to date, skipping DDL")
                schema_reconcile_total.labels(result="skipped").inc()
                return

            log.info(
                "schema hash changed, reconciling",
                previous=current_hash and current_hash[:12],
            )
            tier = _classify_tier(current_hash, inputs)

            if dry_run:
                log.info("[dry-run] would apply DDL", tier=tier)
                schema_reconcile_total.labels(result="skipped").inc()
                return

            await self._apply_ddl(conn, inputs, tier)
            duration = time.monotonic() - t0
            schema_reconcile_total.labels(result="success").inc()
            schema_reconcile_duration_seconds.labels(result="success").observe(duration)
        except Exception:
            duration = time.monotonic() - t0
            schema_reconcile_total.labels(result="error").inc()
            schema_reconcile_duration_seconds.labels(result="error").observe(duration)
            raise
        finally:
            await conn.close()

    async def _apply_ddl(
        self, conn: asyncpg.Connection, inputs: ConfigInputs, tier: int
    ) -> None:
        # 1. Stop affected components
        await component_gate.stop_all(conn)

        # 2. Acquire exclusive advisory locks (blocks until replicas idle)
        await component_gate.acquire_migration_locks(conn, tier)
        try:
            # 3. Apply stubs
            stub_sql = generate_stub_ddl(inputs.mapping)
            await applier.apply_stub_ddl(conn, stub_sql)
            log.info("staging table stubs applied")

            # 4. Alembic
            alembic_runner.run_upgrade(self.cfg.database_dsn)
            log.info("alembic upgrade complete")
            schema_ddl_operations_total.labels(operation="alembic").inc()

            # 5. Generate OSI SQL
            matviews_sql = render_mapping(
                self.cfg.mapping_path, self.cfg.connectors_dir
            )
            pgtrickle_sql = convert_to_pgtrickle(matviews_sql)
            log.info("osi-engine render complete")

            # 6. Validate before touching anything
            await validate_views(conn, pgtrickle_sql)
            log.info("EXPLAIN validation passed")

            # 7. Ensure pg_trickle extension exists
            await applier.ensure_pgtrickle_extension(conn)

            # 8. Apply via create_or_replace (idempotent, OID-preserving)
            await applier.apply_pgtrickle_sql(conn, pgtrickle_sql)

            # 9. Drop orphaned stream tables
            current_names = _extract_stream_table_names(pgtrickle_sql)
            await applier.drop_orphaned_stream_tables(conn, current_names)
            log.info("DDL applied", stream_tables=len(current_names))
            schema_stream_tables.set(len(current_names))

            # 10. Set refresh schedules on leaf delta stream tables
            await applier.set_stream_schedules(conn, current_names)

            # 11. Record new schema version
            for component in ("ingest", "writeback"):
                await component_gate.set_schema_version(
                    conn, component, inputs.schema_hash
                )

        except (OsiError, ValidationError) as e:
            log.error("DDL aborted — old schema intact", error=str(e))
            schema_validation_errors_total.inc()
            raise
        finally:
            await component_gate.release_migration_locks(conn, tier)

        # 10. Resume ingest immediately (staging tables unaffected by view rebuild)
        await component_gate.set_desired(conn, "ingest", "running")
        schema_component_state_changes_total.labels(
            component="ingest", state="running"
        ).inc()

        # 11. Wait for stream tables to be ready before resuming writeback
        ready = await self._wait_for_streams(conn)
        if not ready:
            log.warning(
                "stream tables not ready within timeout — writeback stays stopped",
                timeout=self.cfg.stream_ready_timeout,
            )
            return

        # 12. Resume writeback (shadow or live per policy)
        use_shadow = shadow.should_shadow(self.cfg.shadow_on_change, tier)
        writeback_state = "shadow" if use_shadow else "running"
        await component_gate.set_desired(conn, "writeback", writeback_state)
        schema_component_state_changes_total.labels(
            component="writeback", state=writeback_state
        ).inc()
        log.info("components resumed", writeback=writeback_state)

    async def _wait_for_streams(self, conn: asyncpg.Connection) -> bool:
        """Poll pgtrickle.quick_health until status=OK and stale_tables=0."""
        deadline = time.monotonic() + self.cfg.stream_ready_timeout
        while time.monotonic() < deadline:
            row = await conn.fetchrow(
                "SELECT status, stale_tables, error_tables FROM pgtrickle.quick_health"
            )
            if row["status"] == "OK" and row["stale_tables"] == 0:
                return True
            if row["error_tables"] > 0:
                errors = await conn.fetch(
                    "SELECT check_name, detail FROM pgtrickle.health_check() "
                    "WHERE severity = 'ERROR'"
                )
                log.warning(
                    "stream errors during rebuild", errors=[dict(r) for r in errors]
                )
            await asyncio.sleep(5)
        return False


def _classify_tier(previous_hash: str | None, inputs: ConfigInputs) -> int:
    """Classify the change tier.

    Tier 0: no change (caller should have short-circuited before here)
    Tier 1: views only — writeback stops, ingest continues
    Tier 2: sources + views — both stop
    Tier 3: operational schema (Alembic) — both stop (treated as tier 2 here)

    For the PoC always use tier 2 (stop both) until we implement fine-grained diffing.
    """
    if previous_hash is None:
        return 2  # first deploy
    return 2


def _extract_stream_table_names(sql: str) -> set[str]:
    import re

    pattern = re.compile(r"name\s*=>\s*'([^']+)'", re.IGNORECASE)
    return set(pattern.findall(sql))
