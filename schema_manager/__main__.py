"""CLI entrypoint: schema-manager run | reconcile | promote."""

from __future__ import annotations

import argparse
import asyncio
import sys


def main() -> None:
    parser = argparse.ArgumentParser(prog="schema-manager")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Start the schema-manager service loop")
    run_p.add_argument("--config", default="/config/schema-manager.yaml")
    run_p.add_argument("--dry-run", action="store_true",
                       help="Compute and log DDL diff without applying")

    rec_p = sub.add_parser("reconcile", help="Run a single reconcile and exit")
    rec_p.add_argument("--config", default="/config/schema-manager.yaml")
    rec_p.add_argument("--dry-run", action="store_true")

    prom_p = sub.add_parser("promote", help="Promote writeback from shadow to live")
    prom_p.add_argument("--database", help="Override DSN (else reads from config)")
    prom_p.add_argument("--config", default="/config/schema-manager.yaml")

    args = parser.parse_args()

    if args.command == "run":
        from schema_manager.reconciler import Reconciler
        reconciler = Reconciler.from_config_file(args.config, dry_run=args.dry_run)
        asyncio.run(reconciler.run())

    elif args.command == "reconcile":
        from schema_manager.reconciler import Reconciler
        reconciler = Reconciler.from_config_file(args.config, dry_run=args.dry_run)
        asyncio.run(reconciler.reconcile_once())

    elif args.command == "promote":
        from schema_manager.shadow import promote
        asyncio.run(promote(args.config, dsn_override=args.database))


if __name__ == "__main__":
    sys.exit(main())
