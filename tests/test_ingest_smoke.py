from __future__ import annotations

from datetime import datetime

from cdr_pipeline import ingest


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


class DummySession:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_run_ingest_smoke(monkeypatch):
    conn = DummyConn()
    session = DummySession()
    recorded = {"execute_calls": 0, "execute_batch_calls": 0, "brands_processed": 0}

    def fake_bootstrap_db(force: bool = False) -> None:
        assert force is False

    def fake_connect_with_retries(dsn: str, retries: int = 30, sleep_seconds: float = 1.0, autocommit: bool = False):
        assert "dbname=" in dsn
        assert autocommit is False
        return conn

    def fake_build_session(retry_total: int, backoff_factor: float, user_agent: str):
        assert retry_total >= 0
        assert backoff_factor >= 0
        assert user_agent
        return session

    def fake_execute(_conn, _sql, _params=None):
        recorded["execute_calls"] += 1

    def fake_execute_batch(_conn, _sql, rows):
        rows = list(rows)
        assert rows
        recorded["execute_batch_calls"] += 1

    def fake_discover_brands(_cfg, _session, _conn, _run_id, _run_date):
        return [
            {
                "dataHolderBrandId": "provider-1",
                "brandName": "Provider One",
                "industries": ["banking"],
                "publicBaseUri": "https://example.com",
                "productBaseUri": "https://example.com",
            }
        ]

    def fake_fetch_products_for_brand(_cfg, _session, _conn, _run_id, _run_date, _brand):
        recorded["brands_processed"] += 1
        return 1, {"p1"}

    monkeypatch.setattr(ingest, "bootstrap_db", fake_bootstrap_db)
    monkeypatch.setattr(ingest, "connect_with_retries", fake_connect_with_retries)
    monkeypatch.setattr(ingest, "build_session", fake_build_session)
    monkeypatch.setattr(ingest, "execute", fake_execute)
    monkeypatch.setattr(ingest, "execute_batch", fake_execute_batch)
    monkeypatch.setattr(ingest, "_discover_brands", fake_discover_brands)
    monkeypatch.setattr(ingest, "_fetch_products_for_brand", fake_fetch_products_for_brand)

    ingest.run_ingest(datetime(2026, 2, 10))

    assert recorded["execute_calls"] >= 1
    assert recorded["execute_batch_calls"] == 1
    assert recorded["brands_processed"] == 1
    assert conn.commits >= 2
    assert conn.rollbacks == 0
    assert conn.closed is True
    assert session.closed is True
