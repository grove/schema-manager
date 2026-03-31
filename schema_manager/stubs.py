"""Generate staging table stub DDL from the sources: section of mapping.yaml."""

from __future__ import annotations

from schema_manager.config import MappingConfig


def generate_stub_ddl(mapping: MappingConfig) -> str:
    """Return a SQL string that creates all staging table stubs.

    Stubs use CREATE TABLE IF NOT EXISTS so they are idempotent.
    Ingest will INSERT/UPSERT into these tables; the schema-manager owns
    their creation so there is no chicken-and-egg problem on first deploy.
    """
    statements = []
    for source_name, source_cfg in mapping.sources.items():
        table = _table_name(source_name)
        pk = source_cfg.get("primary_key", "external_id")
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {table} (\n"
            f"    {pk}      TEXT PRIMARY KEY,\n"
            f"    data       JSONB NOT NULL DEFAULT '{{}}',\n"
            f"    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()\n"
            f");"
        )
    return "\n\n".join(statements)


def _table_name(source_name: str) -> str:
    # source names from mapping.yaml are already snake_case table names
    return source_name
