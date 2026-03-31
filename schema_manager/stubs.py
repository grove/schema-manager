"""Generate staging table stub DDL from the sources: section of mapping.yaml.

The generated DDL must match the schema in
    vendor/in-and-out/engine/src/inandout/postgres/schema.py
so that ingest can INSERT/UPSERT without schema drift.
"""

from __future__ import annotations

from schema_manager.config import MappingConfig


def generate_stub_ddl(mapping: MappingConfig) -> str:
    """Return a SQL string that creates all staging table stubs.

    Stubs use CREATE TABLE IF NOT EXISTS so they are idempotent.
    The column set matches the `source_table_ddl()` in the in-and-out engine
    exactly so ingest (in freeze mode) finds all expected columns.
    Also creates lwstate stubs for any mapping with a written_state block so
    that EXPLAIN validation passes before the writeback engine creates them.
    """
    statements = []
    for source_name, source_cfg in mapping.sources.items():
        table = _resolve_table(source_name, source_cfg)
        pk = source_cfg.get("primary_key", "external_id")
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {table} (\n"
            f"    {pk}             TEXT NOT NULL,\n"
            f"    data             JSONB NOT NULL,\n"
            f"    raw              JSONB NOT NULL,\n"
            f"    _ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n"
            f"    _sync_run_id     UUID,\n"
            f"    _raw_hash        TEXT NOT NULL DEFAULT '',\n"
            f"    _deleted         BOOLEAN NOT NULL DEFAULT FALSE,\n"
            f"    _deleted_at      TIMESTAMPTZ,\n"
            f"    _schema_version  INTEGER NOT NULL DEFAULT 1,\n"
            f"    _source_version  TEXT,\n"
            f"    _last_written    JSONB,\n"
            f"    _lineage         JSONB,\n"
            f"    PRIMARY KEY ({pk})\n"
            f");\n"
            f"CREATE INDEX IF NOT EXISTS {table}_ingested_at_idx ON {table} (_ingested_at);\n"
            f"ALTER TABLE {table} OWNER TO sesam_ingest;"
        )

    for m in mapping.raw.get("mappings", []):
        ws = m.get("written_state")
        if not ws:
            continue
        table = ws["table"]
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {table} (\n"
            f"    external_id     TEXT NOT NULL PRIMARY KEY,\n"
            f"    cluster_id      TEXT,\n"
            f"    data            JSONB NOT NULL,\n"
            f"    _etag           TEXT,\n"
            f"    _written_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n"
            f"    _sync_run_id    UUID\n"
            f");"
        )

    return "\n\n".join(statements)


def _resolve_table(source_name: str, source_cfg: dict) -> str:
    """Return the table name from config, falling back to the source name."""
    return source_cfg.get("table", source_name)
