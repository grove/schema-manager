"""Run Alembic migrations via the inandout CLI subprocess."""

from __future__ import annotations

import subprocess


class AlembicError(RuntimeError):
    pass


def run_upgrade(dsn: str) -> None:
    """Run `inandout db upgrade` to apply all pending Alembic revisions."""
    result = subprocess.run(
        ["inandout", "db", "upgrade"],
        env={"INOUT_DATABASE_URL": dsn},
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AlembicError(
            f"inandout db upgrade failed (exit {result.returncode}):\n{result.stderr}"
        )
