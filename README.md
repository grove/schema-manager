# schema-manager

Always-running service that owns the full lifecycle of the database schema and acts as
supervisor for the ingest and writeback components. No other component writes DDL or
manages its own schema.

See the [implementation plan](https://github.com/BaardBouvet/sesam-opensource-poc/blob/main/plans/coordination/schema-manager.md)
for design decisions, architecture, and the phased implementation sequence.

## Quick start (local dev)

```bash
# Install (editable)
pip install -e ".[dev]"

# Run against a local Postgres
export INOUT_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/sesam"
schema-manager run --config /path/to/schema-manager.yaml

# Dry-run (preview DDL without applying)
schema-manager run --config /path/to/schema-manager.yaml --dry-run

# Trigger reconcile manually
schema-manager reconcile --config /path/to/schema-manager.yaml

# Promote writeback from shadow to live
schema-manager promote --database "$INOUT_DATABASE_URL"
```

## Package layout

```
schema_manager/
├── __main__.py          CLI entrypoint (run / reconcile / promote)
├── reconciler.py        Core loop: compute desired state, diff, apply
├── config.py            Read and hash mapping.yaml + connectors
├── stubs.py             Generate staging table stub DDL from sources:
├── osi.py               Call osi-engine render + convert script
├── alembic_runner.py    Call inandout db upgrade as subprocess
├── applier.py           Apply generated SQL via create_or_replace_stream_table()
├── validator.py         EXPLAIN-based validation of generated views
├── component_gate.py    Manage component_state table + advisory lock barrier
├── shadow.py            Shadow mode policy, promotion, shadow_log queries
├── health.py            /health /ready /reconcile /promote HTTP endpoints
├── watcher.py           Watch loop: inotify on /config/ + periodic fallback
└── self_upgrade.py      Version-gated DDL for schema-manager's own tables
```
