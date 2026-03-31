"""Prometheus metrics registry and metric definitions for the schema-manager."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry()


def _counter(name: str, documentation: str, labelnames: list[str]) -> Counter:
    try:
        return Counter(name, documentation, labelnames, registry=REGISTRY)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)  # type: ignore[return-value]


def _gauge(name: str, documentation: str, labelnames: list[str]) -> Gauge:
    try:
        return Gauge(name, documentation, labelnames, registry=REGISTRY)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)  # type: ignore[return-value]


def _histogram(name: str, documentation: str, labelnames: list[str]) -> Histogram:
    try:
        return Histogram(name, documentation, labelnames, registry=REGISTRY)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)  # type: ignore[return-value]


# Full reconcile cycles
schema_reconcile_total: Counter = _counter(
    "schema_reconcile_total",
    "Total reconcile cycles completed",
    ["result"],  # result: success | error | skipped
)

# Reconcile cycle duration
schema_reconcile_duration_seconds: Histogram = _histogram(
    "schema_reconcile_duration_seconds",
    "Duration of reconcile cycles in seconds",
    ["result"],
)

# DDL operations applied
schema_ddl_operations_total: Counter = _counter(
    "schema_ddl_operations_total",
    "Total DDL operations applied by type",
    ["operation"],  # operation: alembic | pgtrickle_apply | stub | orphan_drop
)

# Stream tables currently managed
schema_stream_tables: Gauge = _gauge(
    "schema_stream_tables",
    "Number of pg-trickle stream tables currently managed by schema-manager",
    [],
)

# Component state changes
schema_component_state_changes_total: Counter = _counter(
    "schema_component_state_changes_total",
    "Total component desired-state changes issued by the schema-manager",
    [
        "component",
        "state",
    ],  # component: ingest | writeback; state: running | shadow | stopped
)

# Whether schema-manager currently holds the leader lock (1=yes, 0=no)
schema_leader: Gauge = _gauge(
    "schema_leader",
    "1 if this schema-manager instance holds the leader lock",
    [],
)

# Validation errors (EXPLAIN validation failures before applying DDL)
schema_validation_errors_total: Counter = _counter(
    "schema_validation_errors_total",
    "Total DDL validation errors (EXPLAIN check failed before apply)",
    [],
)
