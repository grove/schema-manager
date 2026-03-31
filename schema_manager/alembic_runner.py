"""Run Alembic migrations via the inandout CLI subprocess."""

from __future__ import annotations

import os
import subprocess
import tempfile

import yaml


class AlembicError(RuntimeError):
    pass


def run_upgrade(dsn: str) -> None:
    """Run `inandout db upgrade` to apply all pending Alembic revisions.

    The inandout CLI is installed in /opt/inandout-venv with migrations in
    /opt/inandout. We create a minimal config file pointing at the DSN and
    run from the /opt/inandout directory (where alembic.ini lives).
    """
    inandout_bin = os.path.join(
        os.environ.get("INANDOUT_VENV", "/opt/inandout-venv"), "bin", "inandout"
    )
    inandout_dir = "/opt/inandout"

    # Create a temp config with just the DSN
    with tempfile.NamedTemporaryFile(
        suffix=".yaml", mode="w", delete=False, prefix="alembic_cfg_"
    ) as f:
        yaml.dump({"database": {"dsn": dsn}}, f)
        tmp_config = f.name

    try:
        result = subprocess.run(
            [inandout_bin, "db", "upgrade", "--config", tmp_config],
            cwd=inandout_dir,
            env={**os.environ, "INOUT_DATABASE_URL": dsn},
            capture_output=True,
            text=True,
        )
    finally:
        os.unlink(tmp_config)

    if result.returncode != 0:
        raise AlembicError(
            f"inandout db upgrade failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
