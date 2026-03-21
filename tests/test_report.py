from __future__ import annotations

from datetime import datetime

from cdr_pipeline import report


class DummyConn:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class DummyConfig:
    def pg_dsn(self) -> str:
        return "dbname=cdr"


def test_run_report_resolves_public_gold_and_filters_requested_date(monkeypatch, tmp_path):
    conn = DummyConn()
    calls: list[tuple[str, tuple | None]] = []

    def fake_load_dotenv(*_args, **_kwargs):
        return None

    def fake_from_env():
        return DummyConfig()

    def fake_connect_with_retries(dsn: str, autocommit: bool = False):
        assert dsn == "dbname=cdr"
        assert autocommit is True
        return conn

    def fake_fetchall(_conn, sql: str, params: tuple | None = None):
        calls.append((sql, params))
        if "to_regclass" in sql:
            relation_name = params[0]
            if relation_name in {"public_gold.mart_rate_changes", "public_gold.mart_provider_coverage"}:
                return [(relation_name,)]
            return [(None,)]
        if "FROM public_gold.mart_rate_changes" in sql:
            assert params == ("2026-02-10",)
            return [("p1", "Brand", "prod", "Product", "cat", "deposit", "fixed", "Tier", "2026-02-09", "2026-02-10", 1.0, 1.2, 0.2)]
        if "FROM public_gold.mart_provider_coverage" in sql:
            assert params == ("2026-02-10",)
            return [("2026-02-10", "p1", "Brand", "https://example.com", 1, 4, 200, None)]
        if "FROM bronze.schema_drift_event" in sql:
            assert params == ("2026-02-10",)
            return [("p1", "banking:get-products", "old", "new", "2026-02-10T01:00:00+00:00")]
        raise AssertionError(sql)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(report, "load_dotenv", fake_load_dotenv)
    monkeypatch.setattr(report.Config, "from_env", staticmethod(fake_from_env))
    monkeypatch.setattr(report, "connect_with_retries", fake_connect_with_retries)
    monkeypatch.setattr(report, "fetchall", fake_fetchall)

    report.run_report(datetime(2026, 2, 10))

    summary = (tmp_path / "reports" / "pipeline_summary_2026-02-10.md").read_text(encoding="utf-8")
    assert "2026-02-10" in summary
    assert "Providers discovered: **1**" in summary
    assert conn.closed is True
    assert any("FROM public_gold.mart_rate_changes" in sql for sql, _ in calls)
    assert any("FROM public_gold.mart_provider_coverage" in sql for sql, _ in calls)

