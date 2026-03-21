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


def _relation_exists(conn, relation_name: str) -> bool:
    rows = fetchall(conn, "SELECT to_regclass(%s)", (relation_name,))
    return bool(rows and rows[0][0] is not None)


def _resolve_relation(conn, candidates: list[str]) -> str | None:
    for relation_name in candidates:
        if _relation_exists(conn, relation_name):
            return relation_name
    return None


def run_report(run_dt: datetime) -> None:
    load_dotenv(override=False)
    cfg = Config.from_env()
    with closing(connect_with_retries(cfg.pg_dsn(), autocommit=True)) as conn:
        report_date = run_dt.strftime("%Y-%m-%d")
        _ensure_dir("reports")

        errors: list[str] = []
        rate_changes_rel = _resolve_relation(conn, ["gold.mart_rate_changes", "public_gold.mart_rate_changes"])
        provider_coverage_rel = _resolve_relation(conn, ["gold.mart_provider_coverage", "public_gold.mart_provider_coverage"])

        if rate_changes_rel is None:
            rate_changes = []
            errors.append("mart_rate_changes not available (expected gold.mart_rate_changes or public_gold.mart_rate_changes)")
        else:
            try:
                rate_changes = fetchall(
                    conn,
                    f"""
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
                    FROM {rate_changes_rel}
                    WHERE current_as_of_date = %s
                    ORDER BY abs(current_rate - previous_rate) DESC NULLS LAST
                    LIMIT 200
                    """,
                    (report_date,),
                )
            except Exception as e:  # noqa: BLE001
                rate_changes = []
                errors.append(f"{rate_changes_rel} not available (run dbt?): {e}")

        if provider_coverage_rel is None:
            coverage = []
            errors.append("mart_provider_coverage not available (expected gold.mart_provider_coverage or public_gold.mart_provider_coverage)")
        else:
            try:
                coverage = fetchall(
                    conn,
                    f"""
                    SELECT
                      as_of_date,
                      provider_id,
                      brand_name,
                      expected_base_uri,
                      products_pages_ok,
                      products_rows,
                      last_http_status,
                      last_error
                    FROM {provider_coverage_rel}
                    WHERE as_of_date = %s
                    ORDER BY brand_name
                    """,
                    (report_date,),
                )
            except Exception as e:  # noqa: BLE001
                coverage = []
                errors.append(f"{provider_coverage_rel} not available (run dbt?): {e}")

        try:
            drift = fetchall(
                conn,
                """
                SELECT provider_id, endpoint, old_fingerprint_hash, new_fingerprint_hash, observed_at
                FROM bronze.schema_drift_event
                WHERE observed_at::date = %s
                ORDER BY observed_at DESC
                LIMIT 50
                """,
                (report_date,),
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
