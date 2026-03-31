"""Read, validate, and hash the mapping + connector config inputs."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ConnectorConfig:
    name: str
    raw: dict[str, Any]


@dataclass
class MappingConfig:
    sources: dict[str, Any]
    targets: dict[str, Any]
    raw: dict[str, Any]


@dataclass
class SchemaManagerConfig:
    database_dsn: str
    mapping_path: Path
    connectors_dir: Path
    health_listen: str = "0.0.0.0:9080"
    poll_interval: int = 30
    stream_ready_timeout: int = 60
    shadow_on_change: str = "always"   # always | new_targets_only | never
    auto_promote_after: str | None = None
    log_format: str = "json"
    log_level: str = "info"
    metrics_enabled: bool = True

    @classmethod
    def from_file(cls, path: str | Path) -> "SchemaManagerConfig":
        raw = _load_yaml(Path(path))
        dsn = os.path.expandvars(raw["database"]["dsn"])
        return cls(
            database_dsn=dsn,
            mapping_path=Path(raw["config_paths"]["mapping"]),
            connectors_dir=Path(raw["config_paths"]["connectors"]),
            health_listen=raw.get("health_server", {}).get("listen", "0.0.0.0:9080"),
            poll_interval=raw.get("watch", {}).get("poll_interval", 30),
            stream_ready_timeout=raw.get("watch", {}).get("stream_ready_timeout", 60),
            shadow_on_change=raw.get("shadow_mode", {}).get("on_change", "always"),
            auto_promote_after=raw.get("shadow_mode", {}).get("auto_promote_after"),
            log_format=raw.get("observability", {}).get("logging", {}).get("format", "json"),
            log_level=raw.get("observability", {}).get("logging", {}).get("level", "info"),
            metrics_enabled=raw.get("observability", {}).get("metrics", {}).get("enabled", True),
        )


@dataclass
class ConfigInputs:
    """All schema-relevant inputs, plus their combined hash."""
    mapping: MappingConfig
    connectors: list[ConnectorConfig] = field(default_factory=list)
    schema_hash: str = ""

    @classmethod
    def load(cls, cfg: SchemaManagerConfig) -> "ConfigInputs":
        mapping_raw = _load_yaml(cfg.mapping_path)
        mapping = MappingConfig(
            sources=mapping_raw.get("sources", {}),
            targets=mapping_raw.get("targets", {}),
            raw=mapping_raw,
        )

        connectors = []
        if cfg.connectors_dir.is_dir():
            for p in sorted(cfg.connectors_dir.glob("*.yaml")):
                raw = _load_yaml(p)
                connectors.append(ConnectorConfig(name=p.stem, raw=raw))

        schema_hash = _compute_hash(mapping_raw, [c.raw for c in connectors])
        return cls(mapping=mapping, connectors=connectors, schema_hash=schema_hash)


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return yaml.safe_load(f) or {}


def _compute_hash(mapping: dict, connectors: list[dict]) -> str:
    """Stable SHA-256 over schema-relevant fields only.

    Excludes runtime-only fields (rate limits, auth tokens, timeouts) so
    unrelated config changes don't trigger a schema reconcile.
    """
    h = hashlib.sha256()
    # mapping sources and targets drive DDL — hash them
    h.update(yaml.dump(mapping.get("sources", {}), sort_keys=True).encode())
    h.update(yaml.dump(mapping.get("targets", {}), sort_keys=True).encode())
    # connector entity/field definitions drive DDL — hash them
    for connector in connectors:
        h.update(yaml.dump(connector.get("entities", {}), sort_keys=True).encode())
        h.update(yaml.dump(connector.get("fields", {}), sort_keys=True).encode())
    return h.hexdigest()
