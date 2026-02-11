from __future__ import annotations

import os
import shlex
import subprocess
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone

from dotenv import load_dotenv

from cdr_pipeline.bootstrap import bootstrap_db
from cdr_pipeline.config import Config
from cdr_pipeline.db import connect_with_retries, execute_batch, fetchall, transaction


@dataclass(frozen=True)
class GateResult:
    name: str
    passed: bool
    actual_value: float | None
    threshold_value: float | None
    details: str


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _clip_text(s: str, max_chars: int = 4000) -> str:
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 3] + "..."


def _run_dbt_tests(command: str) -> tuple[bool, str]:
    parts = shlex.split(command)
    if not parts:
        return False, "dbt test command is empty"

    try:
        cp = subprocess.run(parts, capture_output=True, text=True, check=False)  # noqa: S603
    except FileNotFoundError as e:
        return False, f"dbt test command executable not found: {e}"
    except Exception as e:  # noqa: BLE001
        return False, f"dbt test command failed to run: {e}"

    out = (cp.stdout or "").strip()
    err = (cp.stderr or "").strip()
    combined = "\n".join(x for x in (out, err) if x).strip()
    if not combined:
        combined = "dbt test produced no output."
    prefix = f"exit_code={cp.returncode}"
    return cp.returncode == 0, _clip_text(f"{prefix}\n{combined}")


def _fetch_number(conn, sql: str, params: tuple | None = None) -> float | None:
    rows = fetchall(conn, sql, params)
    if not rows:
        return None
    val = rows[0][0]
    if val is None:
        return None
    return float(val)


def _relation_exists(conn, relation_name: str) -> bool:
    rows = fetchall(conn, "SELECT to_regclass(%s)", (relation_name,))
    return bool(rows and rows[0][0] is not None)


def _resolve_relation(conn, candidates: list[str]) -> str | None:
    for relation_name in candidates:
        if _relation_exists(conn, relation_name):
            return relation_name
    return None


def _gate_min(name: str, actual_value: float | None, threshold_value: float, unit: str = "") -> GateResult:
    if actual_value is None:
        return GateResult(
            name=name,
            passed=False,
            actual_value=None,
            threshold_value=threshold_value,
            details=f"missing metric value; expected >= {threshold_value}{unit}",
        )
    passed = actual_value >= threshold_value
    op = ">=" if passed else "<"
    return GateResult(
        name=name,
        passed=passed,
        actual_value=actual_value,
        threshold_value=threshold_value,
        details=f"{actual_value}{unit} {op} {threshold_value}{unit}",
    )


def _gate_max(name: str, actual_value: float | None, threshold_value: float, unit: str = "") -> GateResult:
    if actual_value is None:
        return GateResult(
            name=name,
            passed=False,
            actual_value=None,
            threshold_value=threshold_value,
            details=f"missing metric value; expected <= {threshold_value}{unit}",
        )
    passed = actual_value <= threshold_value
    op = "<=" if passed else ">"
    return GateResult(
        name=name,
        passed=passed,
        actual_value=actual_value,
        threshold_value=threshold_value,
        details=f"{actual_value}{unit} {op} {threshold_value}{unit}",
    )


def _gate_min_from_query(
    conn,
    *,
    name: str,
    threshold_value: float,
    sql: str,
    params: tuple | None = None,
    unit: str = "",
) -> GateResult:
    try:
        actual_value = _fetch_number(conn, sql, params)
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        return GateResult(
            name=name,
            passed=False,
            actual_value=None,
            threshold_value=threshold_value,
            details=_clip_text(f"query failed: {e}"),
        )
    return _gate_min(name, actual_value, threshold_value, unit=unit)


def _gate_max_from_query(
    conn,
    *,
    name: str,
    threshold_value: float,
    sql: str,
    params: tuple | None = None,
    unit: str = "",
) -> GateResult:
    try:
        actual_value = _fetch_number(conn, sql, params)
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        return GateResult(
            name=name,
            passed=False,
            actual_value=None,
            threshold_value=threshold_value,
            details=_clip_text(f"query failed: {e}"),
        )
    return _gate_max(name, actual_value, threshold_value, unit=unit)


def run_qa(
    run_dt: datetime,
    *,
    min_providers_ok: int | None = None,
    min_products: int | None = None,
    min_rate_changes: int | None = None,
    max_freshness_hours: float | None = None,
    fail_on_schema_drift: bool | None = None,
    run_dbt_tests: bool | None = None,
    dbt_test_command: str | None = None,
) -> int:
    load_dotenv(override=False)
    cfg = Config.from_env()
    bootstrap_db(force=False)

    threshold_min_providers = min_providers_ok if min_providers_ok is not None else cfg.qa_min_providers_ok
    threshold_min_products = min_products if min_products is not None else cfg.qa_min_products
    threshold_min_rate_changes = min_rate_changes if min_rate_changes is not None else cfg.qa_min_rate_changes
    threshold_max_freshness_hours = max_freshness_hours if max_freshness_hours is not None else cfg.qa_max_freshness_hours
    threshold_fail_on_schema_drift = fail_on_schema_drift if fail_on_schema_drift is not None else cfg.qa_fail_on_schema_drift
    should_run_dbt_tests = run_dbt_tests if run_dbt_tests is not None else cfg.qa_run_dbt_tests
    effective_dbt_test_command = dbt_test_command if dbt_test_command is not None else cfg.qa_dbt_test_command

    qa_date = run_dt.date()
    now_utc = datetime.now(timezone.utc)
    qa_run_id = str(uuid.uuid4())

    dbt_passed = True
    dbt_details = "dbt test skipped"
    if should_run_dbt_tests:
        dbt_passed, dbt_details = _run_dbt_tests(effective_dbt_test_command)

    gate_results: list[GateResult] = []

    with closing(connect_with_retries(cfg.pg_dsn(), autocommit=False)) as conn:
        provider_coverage_rel = _resolve_relation(conn, ["gold.mart_provider_coverage", "public_gold.mart_provider_coverage"])
        if provider_coverage_rel:
            gate_results.append(
                _gate_min_from_query(
                    conn,
                    name="providers_ok",
                    threshold_value=float(threshold_min_providers),
                    sql=f"""
                    SELECT COUNT(*)
                    FROM {provider_coverage_rel}
                    WHERE as_of_date = %s
                      AND COALESCE(products_pages_ok, 0) > 0
                      AND COALESCE(last_http_status, 0) IN (200, 304)
                    """,
                    params=(qa_date,),
                )
            )
        else:
            gate_results.append(
                GateResult(
                    name="providers_ok",
                    passed=False,
                    actual_value=None,
                    threshold_value=float(threshold_min_providers),
                    details="missing relation: expected gold.mart_provider_coverage or public_gold.mart_provider_coverage",
                )
            )

        dim_products_rel = _resolve_relation(conn, ["silver.dim_products", "public_silver.dim_products"])
        if dim_products_rel:
            gate_results.append(
                _gate_min_from_query(
                    conn,
                    name="dim_products_rows",
                    threshold_value=float(threshold_min_products),
                    sql=f"""
                    SELECT COUNT(*)
                    FROM {dim_products_rel}
                    WHERE as_of_date = %s
                    """,
                    params=(qa_date,),
                )
            )
        else:
            gate_results.append(
                GateResult(
                    name="dim_products_rows",
                    passed=False,
                    actual_value=None,
                    threshold_value=float(threshold_min_products),
                    details="missing relation: expected silver.dim_products or public_silver.dim_products",
                )
            )

        rate_changes_rel = _resolve_relation(conn, ["gold.mart_rate_changes", "public_gold.mart_rate_changes"])
        if rate_changes_rel:
            gate_results.append(
                _gate_min_from_query(
                    conn,
                    name="rate_changes_rows",
                    threshold_value=float(threshold_min_rate_changes),
                    sql=f"""
                    SELECT COUNT(*)
                    FROM {rate_changes_rel}
                    WHERE current_as_of_date = %s
                    """,
                    params=(qa_date,),
                )
            )
        else:
            gate_results.append(
                GateResult(
                    name="rate_changes_rows",
                    passed=False,
                    actual_value=None,
                    threshold_value=float(threshold_min_rate_changes),
                    details="missing relation: expected gold.mart_rate_changes or public_gold.mart_rate_changes",
                )
            )

        gate_results.append(
            _gate_max_from_query(
                conn,
                name="products_freshness_hours",
                threshold_value=float(threshold_max_freshness_hours),
                sql="""
                SELECT EXTRACT(EPOCH FROM ((%s::timestamptz) - MAX(fetched_at))) / 3600.0
                FROM raw.products_raw
                """,
                params=(now_utc,),
                unit="h",
            )
        )

        drift_events: float | None
        try:
            drift_events = _fetch_number(
                conn,
                """
                SELECT COUNT(*)
                FROM bronze.schema_drift_event
                WHERE observed_at::date = %s
                """,
                (qa_date,),
            )
        except Exception as e:  # noqa: BLE001
            conn.rollback()
            drift_events = None
            gate_results.append(
                GateResult(
                    name="schema_drift_events",
                    passed=False,
                    actual_value=None,
                    threshold_value=0.0 if threshold_fail_on_schema_drift else None,
                    details=_clip_text(f"query failed: {e}"),
                )
            )
            drift_events = None

        drift_threshold = 0.0 if threshold_fail_on_schema_drift else float("inf")
        if not any(gr.name == "schema_drift_events" for gr in gate_results) and threshold_fail_on_schema_drift:
            gate_results.append(_gate_max("schema_drift_events", drift_events, drift_threshold))
        elif not any(gr.name == "schema_drift_events" for gr in gate_results):
            gate_results.append(
                GateResult(
                    name="schema_drift_events",
                    passed=True,
                    actual_value=drift_events,
                    threshold_value=None,
                    details=f"observed={drift_events}; fail gate disabled",
                )
            )

        dbt_gate = GateResult(
            name="dbt_tests",
            passed=dbt_passed,
            actual_value=1.0 if dbt_passed else 0.0,
            threshold_value=1.0,
            details=dbt_details if should_run_dbt_tests else "skipped",
        )
        gate_results.append(dbt_gate)

        with transaction(conn):
            rows = [
                (
                    qa_run_id,
                    qa_date,
                    now_utc,
                    gr.name,
                    gr.passed,
                    gr.actual_value,
                    gr.threshold_value,
                    gr.details,
                    should_run_dbt_tests,
                    dbt_passed,
                    effective_dbt_test_command if should_run_dbt_tests else None,
                )
                for gr in gate_results
            ]
            execute_batch(
                conn,
                """
                INSERT INTO bronze.qa_gate_result (
                    qa_run_id, qa_date, evaluated_at, gate_name, passed, actual_value, threshold_value, details,
                    dbt_test_ran, dbt_test_passed, dbt_test_command
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )

    _ensure_dir("reports")
    qa_date_str = qa_date.strftime("%Y-%m-%d")
    summary_path = os.path.join("reports", f"qa_summary_{qa_date_str}.md")
    failed = [gr for gr in gate_results if not gr.passed]
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"# QA summary - {qa_date_str}\n\n")
        status = "PASS" if not failed else "FAIL"
        f.write(f"- Status: **{status}**\n")
        f.write(f"- QA run id: `{qa_run_id}`\n")
        f.write(f"- dbt tests: **{'PASS' if dbt_passed else 'FAIL'}**")
        if should_run_dbt_tests:
            f.write(f" (`{effective_dbt_test_command}`)\n\n")
        else:
            f.write(" (skipped)\n\n")

        f.write("| Gate | Passed | Actual | Threshold | Details |\n")
        f.write("|---|---|---:|---:|---|\n")
        for gr in gate_results:
            actual = "" if gr.actual_value is None else str(gr.actual_value)
            threshold = "" if gr.threshold_value is None else str(gr.threshold_value)
            f.write(f"| {gr.name} | {'yes' if gr.passed else 'no'} | {actual} | {threshold} | {gr.details} |\n")

    print(f"Wrote QA summary: {os.path.abspath(summary_path)}")
    if failed:
        print("QA failed gates:")
        for gr in failed:
            print(f"- {gr.name}: {gr.details}")
        return 1
    print("QA gates passed.")
    return 0
