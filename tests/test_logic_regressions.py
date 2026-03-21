from __future__ import annotations

from datetime import datetime
from pathlib import Path

from cdr_pipeline import bootstrap, qa
from cdr_pipeline.qa import GateResult


class DummyConn:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class DummyConfig:
    qa_min_providers_ok = 1
    qa_min_products = 1
    qa_min_rate_changes = 1
    qa_max_freshness_hours = 36.0
    qa_fail_on_schema_drift = False
    qa_run_dbt_tests = True
    qa_dbt_test_command = "dbt test --project-dir dbt --profiles-dir dbt"

    def pg_dsn(self) -> str:
        return "dbname=cdr"


def test_bootstrap_force_drops_public_dbt_schemas(monkeypatch):
    conn = DummyConn()
    executed: list[str] = []

    def fake_from_env():
        return DummyConfig()

    def fake_connect_with_retries(dsn: str, autocommit: bool = False):
        assert dsn == "dbname=cdr"
        assert autocommit is False
        return conn

    def fake_execute(_conn, sql: str, params=None):
        executed.append(sql)

    monkeypatch.setattr(bootstrap.Config, "from_env", staticmethod(fake_from_env))
    monkeypatch.setattr(bootstrap, "connect_with_retries", fake_connect_with_retries)
    monkeypatch.setattr(bootstrap, "execute", fake_execute)

    bootstrap.bootstrap_db(force=True)

    assert "DROP SCHEMA IF EXISTS public_gold CASCADE;" in executed
    assert "DROP SCHEMA IF EXISTS public_silver CASCADE;" in executed
    assert "DROP SCHEMA IF EXISTS public_staging CASCADE;" in executed
    assert conn.commits == 1
    assert conn.closed is True


def test_qa_freshness_gate_is_scoped_to_requested_date(monkeypatch, tmp_path):
    conn = DummyConn()
    captured: dict[str, tuple | None] = {}

    def fake_load_dotenv(*_args, **_kwargs):
        return None

    def fake_from_env():
        return DummyConfig()

    def fake_bootstrap_db(force: bool = False):
        assert force is False

    def fake_connect_with_retries(dsn: str, autocommit: bool = False):
        assert dsn == "dbname=cdr"
        assert autocommit is False
        return conn

    def fake_resolve_relation(_conn, candidates):
        return None

    def fake_gate_max_from_query(_conn, *, name: str, threshold_value: float, sql: str, params=None, unit: str = ""):
        captured["freshness"] = params
        assert "WHERE fetched_at::date = %s" in sql
        return GateResult(name=name, passed=True, actual_value=1.0, threshold_value=threshold_value, details="ok")

    def fake_fetch_number(_conn, sql: str, params=None):
        assert "schema_drift_event" in sql
        return 0.0

    def fake_execute_batch(_conn, _sql: str, rows):
        assert list(rows)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(qa, "load_dotenv", fake_load_dotenv)
    monkeypatch.setattr(qa.Config, "from_env", staticmethod(fake_from_env))
    monkeypatch.setattr(qa, "bootstrap_db", fake_bootstrap_db)
    monkeypatch.setattr(qa, "connect_with_retries", fake_connect_with_retries)
    monkeypatch.setattr(qa, "_resolve_relation", fake_resolve_relation)
    monkeypatch.setattr(qa, "_gate_max_from_query", fake_gate_max_from_query)
    monkeypatch.setattr(qa, "_fetch_number", fake_fetch_number)
    monkeypatch.setattr(qa, "execute_batch", fake_execute_batch)

    rc = qa.run_qa(datetime(2026, 2, 10), run_dbt_tests=False)

    assert rc == 1
    assert captured["freshness"][1].isoformat() == "2026-02-10"
    assert captured["freshness"][0].isoformat() == "2026-02-11T00:00:00+00:00"
    assert conn.closed is True


def test_mart_provider_coverage_tracks_all_run_dates():
    sql = Path("dbt/models/gold/mart_provider_coverage.sql").read_text(encoding="utf-8")

    assert "select distinct run_date as as_of_date" in sql
    assert "join brands b on b.as_of_date = d.as_of_date" in sql
    assert "join {{ source('bronze','pipeline_run') }} pr" in sql
    assert "partition by as_of_date, provider_id" in sql
    assert "p.as_of_date = d.as_of_date" in sql
    assert "max(fetched_at::date)" not in sql


def test_mart_rate_changes_matches_tiers_on_bounds_and_units():
    sql = Path("dbt/models/gold/mart_rate_changes.sql").read_text(encoding="utf-8")

    assert "lag(as_of_date) over" in sql
    assert "cur.current_date = prv.current_date" in sql
    assert "tier_unit_of_measure" in sql
    assert "tier_minimum_value" in sql
    assert "tier_maximum_value" in sql
    assert "coalesce(cur.tier_unit_of_measure,'') = coalesce(prv.tier_unit_of_measure,'')" in sql
    assert "coalesce(cur.tier_minimum_value, -1) = coalesce(prv.tier_minimum_value, -1)" in sql
    assert "coalesce(cur.tier_maximum_value, -1) = coalesce(prv.tier_maximum_value, -1)" in sql


def test_schema_tracks_provider_coverage_uniqueness_by_date_and_provider():
    schema = Path("dbt/models/schema.yml").read_text(encoding="utf-8")

    assert "- name: mart_provider_coverage" in schema
    assert "- as_of_date" in schema
    assert "- provider_id" in schema
    assert "tests: [not_null, unique]" not in schema
