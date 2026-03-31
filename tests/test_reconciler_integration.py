"""Phase 1 integration test: reconciler core against a real Postgres.

Requires: pytest, pytest-asyncio, testcontainers[postgres]
Run: pytest tests/test_reconciler_integration.py -v
"""

from __future__ import annotations

import asyncpg
import pytest
import yaml
from testcontainers.postgres import PostgresContainer

from schema_manager.component_gate import (
    self_upgrade as gate_self_upgrade,
    set_desired,
    get_schema_version,
    set_schema_version,
    try_become_leader,
    stop_all,
)
from schema_manager.config import ConfigInputs, SchemaManagerConfig
from schema_manager.self_upgrade import run as self_upgrade_run
from schema_manager.shadow import self_upgrade as shadow_self_upgrade, should_shadow
from schema_manager.stubs import generate_stub_ddl


MINIMAL_MAPPING = {
    "sources": {
        "hubspot_contacts": {"primary_key": "external_id"},
        "tripletex_employees": {"primary_key": "external_id"},
    },
    "targets": {},
}

MINIMAL_CONNECTOR = {
    "entities": {"contacts": {"source": "hubspot_contacts"}},
}


@pytest.fixture(scope="module")
def postgres_dsn():
    with PostgresContainer("postgres:16") as pg:
        # testcontainers gives a psycopg2 URL; convert to raw for asyncpg
        url = pg.get_connection_url()
        # postgresql+psycopg2://user:pass@host:port/db -> postgresql://user:pass@host:port/db
        raw = url.split("://", 1)[1]
        yield f"postgresql://{raw}"


@pytest.fixture
def config_dir(tmp_path):
    mapping_path = tmp_path / "mapping.yaml"
    mapping_path.write_text(yaml.dump(MINIMAL_MAPPING))
    connectors_dir = tmp_path / "connectors"
    connectors_dir.mkdir()
    (connectors_dir / "hubspot.yaml").write_text(yaml.dump(MINIMAL_CONNECTOR))
    return tmp_path


@pytest.mark.asyncio
async def test_config_loads_and_hashes(config_dir, postgres_dsn):
    """Config loading produces a stable hash and parses sources."""
    cfg = SchemaManagerConfig(
        database_dsn=postgres_dsn,
        mapping_path=config_dir / "mapping.yaml",
        connectors_dir=config_dir / "connectors",
    )
    inputs = ConfigInputs.load(cfg)
    assert inputs.schema_hash
    assert len(inputs.schema_hash) == 64  # SHA-256 hex
    assert "hubspot_contacts" in inputs.mapping.sources
    assert "tripletex_employees" in inputs.mapping.sources

    # Hash is deterministic
    inputs2 = ConfigInputs.load(cfg)
    assert inputs.schema_hash == inputs2.schema_hash


@pytest.mark.asyncio
async def test_stub_ddl_creates_tables(config_dir, postgres_dsn):
    """Stub DDL creates all source tables and is idempotent."""
    cfg = SchemaManagerConfig(
        database_dsn=postgres_dsn,
        mapping_path=config_dir / "mapping.yaml",
        connectors_dir=config_dir / "connectors",
    )
    inputs = ConfigInputs.load(cfg)
    stub_sql = generate_stub_ddl(inputs.mapping)
    assert "hubspot_contacts" in stub_sql
    assert "tripletex_employees" in stub_sql
    assert "CREATE TABLE IF NOT EXISTS" in stub_sql

    conn = await asyncpg.connect(postgres_dsn)
    try:
        await conn.execute(stub_sql)
        # Both tables should exist
        result = await conn.fetchval(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name IN ('hubspot_contacts', 'tripletex_employees')"
        )
        assert result == 2

        # Running stubs again should be a no-op (IF NOT EXISTS)
        await conn.execute(stub_sql)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_self_upgrade_is_idempotent(postgres_dsn):
    """All self-upgrade functions can be called repeatedly without error."""
    conn = await asyncpg.connect(postgres_dsn)
    try:
        await self_upgrade_run(conn)
        await self_upgrade_run(conn)
        await gate_self_upgrade(conn)
        await gate_self_upgrade(conn)
        await shadow_self_upgrade(conn)
        await shadow_self_upgrade(conn)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_component_state_transitions(postgres_dsn):
    """component_state table supports upsert and transitions between states."""
    conn = await asyncpg.connect(postgres_dsn)
    try:
        await gate_self_upgrade(conn)

        # Initial insert
        await set_desired(conn, "ingest", "stopped")
        await set_desired(conn, "writeback", "stopped")

        state = await conn.fetchval(
            "SELECT desired FROM component_state WHERE component = 'ingest'"
        )
        assert state == "stopped"

        # Transition to running
        await set_desired(conn, "ingest", "running")
        state = await conn.fetchval(
            "SELECT desired FROM component_state WHERE component = 'ingest'"
        )
        assert state == "running"

        # Shadow mode for writeback
        await set_desired(conn, "writeback", "shadow")
        state = await conn.fetchval(
            "SELECT desired FROM component_state WHERE component = 'writeback'"
        )
        assert state == "shadow"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_schema_version_tracking(postgres_dsn, config_dir):
    """Schema version hash is stored per component and can be read back."""
    conn = await asyncpg.connect(postgres_dsn)
    try:
        await gate_self_upgrade(conn)
        await set_desired(conn, "ingest", "stopped")

        # Initially no version
        ver = await get_schema_version(conn, "ingest")
        assert ver is None

        # Set and read back
        await set_schema_version(conn, "ingest", "abc123")
        ver = await get_schema_version(conn, "ingest")
        assert ver == "abc123"

        # Update
        await set_schema_version(conn, "ingest", "def456")
        ver = await get_schema_version(conn, "ingest")
        assert ver == "def456"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_leader_election(postgres_dsn):
    """Only one connection can hold the leader lock."""
    conn1 = await asyncpg.connect(postgres_dsn)
    conn2 = await asyncpg.connect(postgres_dsn)
    try:
        # First claimant wins
        assert await try_become_leader(conn1) is True
        # Second claimant is rejected
        assert await try_become_leader(conn2) is False
    finally:
        await conn1.close()  # releases leader lock
        await conn2.close()

    # After leader disconnects, a new connection can claim it
    conn3 = await asyncpg.connect(postgres_dsn)
    try:
        assert await try_become_leader(conn3) is True
    finally:
        await conn3.close()


@pytest.mark.asyncio
async def test_stop_all(postgres_dsn):
    """stop_all sets both components to stopped."""
    conn = await asyncpg.connect(postgres_dsn)
    try:
        await gate_self_upgrade(conn)
        await set_desired(conn, "ingest", "running")
        await set_desired(conn, "writeback", "running")

        await stop_all(conn)

        for component in ("ingest", "writeback"):
            state = await conn.fetchval(
                "SELECT desired FROM component_state WHERE component = $1", component
            )
            assert state == "stopped"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_shadow_log_table(postgres_dsn):
    """shadow_log table is created and writable."""
    conn = await asyncpg.connect(postgres_dsn)
    try:
        await shadow_self_upgrade(conn)
        await conn.execute(
            "INSERT INTO shadow_log (target, operation, external_id, payload) "
            "VALUES ('hubspot_contacts', 'create', 'ext-001', '{\"name\": \"test\"}')"
        )
        count = await conn.fetchval("SELECT count(*) FROM shadow_log")
        assert count == 1
    finally:
        await conn.close()


def test_should_shadow_policy():
    """Shadow policy correctly gates on tier and policy."""
    assert should_shadow("always", 1) is True
    assert should_shadow("always", 2) is True
    assert should_shadow("never", 1) is False
    assert should_shadow("never", 2) is False
    assert should_shadow("new_targets_only", 1) is False
    assert should_shadow("new_targets_only", 2) is True


@pytest.mark.asyncio
async def test_full_bootstrap_sequence(config_dir, postgres_dsn):
    """End-to-end bootstrap: self-upgrade → stubs → component state.

    This exercises the schema-manager startup path minus osi-engine/Alembic
    (which need external binaries). Proves the core DDL pipeline works.
    """
    conn = await asyncpg.connect(postgres_dsn)
    try:
        # 1. Self-upgrade (schema-manager's own tables)
        await self_upgrade_run(conn)
        await gate_self_upgrade(conn)
        await shadow_self_upgrade(conn)

        # 2. Leader lock
        assert await try_become_leader(conn) is True

        # 3. Stop components
        await stop_all(conn)

        # 4. Apply stubs
        cfg = SchemaManagerConfig(
            database_dsn=postgres_dsn,
            mapping_path=config_dir / "mapping.yaml",
            connectors_dir=config_dir / "connectors",
        )
        inputs = ConfigInputs.load(cfg)
        stub_sql = generate_stub_ddl(inputs.mapping)
        await conn.execute(stub_sql)

        # 5. Record schema version
        for component in ("ingest", "writeback"):
            await set_schema_version(conn, component, inputs.schema_hash)

        # 6. Resume
        await set_desired(conn, "ingest", "running")
        await set_desired(conn, "writeback", "running")

        # Verify final state
        for component in ("ingest", "writeback"):
            state = await conn.fetchval(
                "SELECT desired FROM component_state WHERE component = $1", component
            )
            assert state == "running"
            ver = await get_schema_version(conn, component)
            assert ver == inputs.schema_hash

        # Verify tables exist
        tables = await conn.fetchval(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name IN ('hubspot_contacts', 'tripletex_employees', "
            "'component_state', 'migration_state', 'shadow_log', 'schema_manager_version')"
        )
        assert tables == 6
    finally:
        await conn.close()
