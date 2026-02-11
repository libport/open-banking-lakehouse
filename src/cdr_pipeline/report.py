from __future__ import annotations

import csv
import os
from contextlib import closing
from datetime import datetime

from dotenv import load_dotenv

from cdr_pipeline.config import Config
from cdr_pipeline.db import connect_with_retries, fetchall


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _write_csv(path: str, headers: list[str], rows: list[tuple]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow(list(r))


def run_report(run_dt: datetime) -> None:
    load_dotenv(override=False)
    cfg = Config.from_env()
    with closing(connect_with_retries(cfg.pg_dsn(), autocommit=True)) as conn:
        report_date = run_dt.strftime("%Y-%m-%d")
        _ensure_dir("reports")

        errors: list[str] = []

        try:
            rate_changes = fetchall(
                conn,
                """
                SELECT
                  provider_id,
                  brand_name,
                  product_id,
                  product_name,
                  product_category,
                  rate_kind,
                  rate_type,
                  tier_name,
                  previous_as_of_date,
                  current_as_of_date,
                  previous_rate,
                  current_rate,
                  (current_rate - previous_rate) AS delta
                FROM gold.mart_rate_changes
                ORDER BY abs(current_rate - previous_rate) DESC NULLS LAST
                LIMIT 200
                """,
            )
        except Exception as e:  # noqa: BLE001
            rate_changes = []
            errors.append(f"gold.mart_rate_changes not available (run dbt?): {e}")

        try:
            coverage = fetchall(
                conn,
                """
                SELECT
                  as_of_date,
                  provider_id,
                  brand_name,
                  expected_base_uri,
                  products_pages_ok,
                  products_rows,
                  last_http_status,
                  last_error
                FROM gold.mart_provider_coverage
                ORDER BY brand_name
                """,
            )
        except Exception as e:  # noqa: BLE001
            coverage = []
            errors.append(f"gold.mart_provider_coverage not available (run dbt?): {e}")

        try:
            drift = fetchall(
                conn,
                """
                SELECT provider_id, endpoint, old_fingerprint_hash, new_fingerprint_hash, observed_at
                FROM bronze.schema_drift_event
                ORDER BY observed_at DESC
                LIMIT 50
                """,
            )
        except Exception as e:  # noqa: BLE001
            drift = []
            errors.append(f"bronze.schema_drift_event not available: {e}")

    if rate_changes:
        rate_csv = os.path.join("reports", f"rate_changes_{report_date}.csv")
        _write_csv(
            rate_csv,
            [
                "provider_id",
                "brand_name",
                "product_id",
                "product_name",
                "product_category",
                "rate_kind",
                "rate_type",
                "tier_name",
                "previous_as_of_date",
                "current_as_of_date",
                "previous_rate",
                "current_rate",
                "delta",
            ],
            rate_changes,
        )

    if coverage:
        cov_csv = os.path.join("reports", f"provider_coverage_{report_date}.csv")
        _write_csv(
            cov_csv,
            ["as_of_date", "provider_id", "brand_name", "expected_base_uri", "products_pages_ok", "products_rows", "last_http_status", "last_error"],
            coverage,
        )

    md_path = os.path.join("reports", f"pipeline_summary_{report_date}.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Pipeline summary — {report_date}\n\n")

        if errors:
            f.write("## Notes\n\n")
            for e in errors:
                f.write(f"- {e}\n")
            f.write("\n")

        if coverage:
            total = len(coverage)
            ok = sum(1 for r in coverage if (r[6] in (200, 304)) and (r[4] or 0) > 0)
            f.write("## Coverage\n\n")
            f.write(f"- Providers discovered: **{total}**\n")
            f.write(f"- Providers with OK product fetch: **{ok}**\n\n")

        if rate_changes:
            f.write("## Top rate changes (max 20)\n\n")
            f.write("| Brand | Product | Category | Rate type | Tier | Previous | Current | Δ |\n")
            f.write("|---|---|---|---|---:|---:|---:|---:|\n")
            for r in rate_changes[:20]:
                brand = r[1]
                product = r[3] or r[2]
                cat = r[4] or ""
                rate_type = f"{r[5]}/{r[6]}"
                tier = r[7] or ""
                prev = r[10]
                cur = r[11]
                delta = r[12]
                f.write(f"| {brand} | {product} | {cat} | {rate_type} | {tier} | {prev} | {cur} | {delta} |\n")
            f.write("\n")

        if drift:
            f.write("## Schema drift events (last 10)\n\n")
            for r in drift[:10]:
                f.write(f"- {r[4]} — provider={r[0]} endpoint={r[1]} old={r[2]} new={r[3]}\n")
            f.write("\n")

    print(f"Wrote reports to: {os.path.abspath('reports')}")
    print(f"- {md_path}")
