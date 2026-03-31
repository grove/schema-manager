"""Phase 1 integration test: reconciler core against a real Postgres.

Requires: pytest, pytest-asyncio, testcontainers[postgres]
Run: pytest tests/test_reconciler_integration.py -v
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from textwrap import dedent

import asyncpg
import pytest
import yaml
from testcontainers.postgres import PostgresContainer

from schema_manager.component_gate import self_upgrade as gate_self_upgrade, set_desired
from schema_manager.config import ConfigInputs, SchemaManagerConfig
from schema_manager.self_upgrade import run as self_upgrade_run
from schema_manager.stubs import generate_stub_ddl


MINIMAL_MAPPING = {
    "sources": {
        "hubspot_contacts": {"primary_key": "external_id"},
    },
    "targets": {},
}

MINIMAL_CONNECTOR = {
    "entities": {"contacts": {"source": "hubspot_contacts"}},
}


@pytest.fixture(scope="module")
def postgres_dsn(tmp_path_factory):
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url().replace("psycopg2", "asyncpg").replace("+asyncpg", "")


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
    cfg = SchemaManagerConfig(
        database_dsn=postgres_dsn,
        mapping_path=config_dir / "mapping.yaml",
        connectors_dir=config_dir / "connectors",
    )
    inputs = ConfigInputs.load(cfg)
    assert inputs.schema_hash
    assert "hubspot_contacts" in inputs.mapping.sources


@pytest.mark.asyncio
async def test_stub_ddl_creates_tables(config_dir, postgres_dsn):
    cfg = SchemaManagerConfig(
        database_dsn=postgres_dsn,
        mapping_path=config_dir / "mapping.yaml",
        connectors_dir=config_dir / "connectors",
    )
    inputs = ConfigInputs.load(cfg)
    stub_sql = generate_stub_ddl(inputs.mapping)
    assert "hubspot_contacts" in stub_sql
    assert "CREATE TABLE IF NOT EXISTS" in stub_sql

    conn = await asyncpg.connect(postgres_dsn)
    try:
        await self_upgrade_run(conn)
        await gate_self_upgrade(conn)
        await conn.execute(stub_sql)
        # Table should now exist
        result = await conn.fetchval(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name = 'hubspot_contacts'"
        )
        assert result == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_self_upgrade_is_idempotent(postgres_dsn):
    conn = await asyncpg.connect(postgres_dsn)
    try:
        # Running twice should not fail
        await self_upgrade_run(conn)
        await self_upgrade_run(conn)
        await gate_self_upgrade(conn)
        await gate_self_upgrade(conn)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_component_state_transitions(postgres_dsn):
    conn = await asyncpg.connect(postgres_dsn)
    try:
        await gate_self_upgrade(conn)
        await set_desired(conn, "ingest", "stopped")
        await set_desired(conn, "writeback", "stopped")

        state = await conn.fetchrow(
            "SELECT desired FROM component_state WHERE component = 'ingest'"
        )
        assert state["desired"] == "stopped"

        await set_desired(conn, "ingest", "running")
        state = await conn.fetchrow(
            "SELECT desired FROM component_state WHERE component = 'ingest'"
        )
        assert state["desired"] == "running"
    finally:
        await conn.close()
