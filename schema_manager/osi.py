"""Call osi-engine render and the matviews→pgtrickle conversion script."""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path


class OsiError(RuntimeError):
    pass


def render_mapping(mapping_path: Path, connectors_dir: Path) -> str:
    """Run osi-engine render and return the generated matviews SQL."""
    result = subprocess.run(
        ["osi-engine", "render", str(mapping_path), "--connectors", str(connectors_dir)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise OsiError(
            f"osi-engine render failed (exit {result.returncode}):\n{result.stderr}"
        )
    return result.stdout


def convert_to_pgtrickle(matviews_sql: str) -> str:
    """Run convert_matviews_to_pgtrickle.py and return the pg-trickle SQL."""
    with tempfile.NamedTemporaryFile(suffix=".sql", mode="w", delete=False) as f:
        f.write(matviews_sql)
        tmp_path = f.name

    result = subprocess.run(
        ["python", "/usr/local/bin/convert_matviews_to_pgtrickle.py", tmp_path],
        capture_output=True,
        text=True,
    )
    Path(tmp_path).unlink(missing_ok=True)

    if result.returncode != 0:
        raise OsiError(
            f"convert_matviews_to_pgtrickle failed (exit {result.returncode}):\n{result.stderr}"
        )
    return result.stdout
