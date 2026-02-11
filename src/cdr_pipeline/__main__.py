"""
cdr_pipeline package entrypoint.

Run (inside the pipeline container):
  python -m cdr_pipeline ingest --date 2026-02-10
  python -m cdr_pipeline report --date 2026-02-10
"""
from __future__ import annotations

import argparse
from datetime import datetime

from cdr_pipeline.bootstrap import bootstrap_db
from cdr_pipeline.ingest import run_ingest
from cdr_pipeline.report import run_report


def _parse_date(s: str | None) -> datetime:
    if not s:
        return datetime.utcnow()
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return datetime.fromisoformat(s + "T00:00:00")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cdr_pipeline",
        description="AU CDR Open Banking product lakehouse pipeline (local).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_boot = sub.add_parser("bootstrap", help="Create required Postgres schemas/tables.")
    p_boot.add_argument("--force", action="store_true", help="Drop and recreate objects (DANGEROUS).")

    p_ing = sub.add_parser("ingest", help="Discover brands via register and ingest product payloads.")
    p_ing.add_argument("--date", default=None, help="Run date (YYYY-MM-DD). Used for partitioning raw files and report names.")
    p_ing.add_argument("--provider-limit", type=int, default=None, help="Limit number of providers processed (overrides env PROVIDER_LIMIT).")

    p_rep = sub.add_parser("report", help="Generate reports from gold marts (after dbt build).")
    p_rep.add_argument("--date", default=None, help="Report date (YYYY-MM-DD).")

    args = parser.parse_args(argv)

    if args.cmd == "bootstrap":
        bootstrap_db(force=args.force)
        return 0

    if args.cmd == "ingest":
        run_ingest(_parse_date(args.date), provider_limit=args.provider_limit)
        return 0

    if args.cmd == "report":
        run_report(_parse_date(args.date))
        return 0

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
