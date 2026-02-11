from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Sequence

import psycopg2
import psycopg2.extras


def connect_with_retries(
    dsn: str,
    retries: int = 30,
    sleep_seconds: float = 1.0,
    autocommit: bool = False,
):
    last_err: Exception | None = None
    for _ in range(retries):
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = autocommit
            return conn
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Failed to connect to Postgres after {retries} retries: {last_err}") from last_err


@contextmanager
def get_cursor(conn) -> Iterator[psycopg2.extensions.cursor]:
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def execute(conn, sql: str, params: Sequence[Any] | None = None) -> None:
    with get_cursor(conn) as cur:
        cur.execute(sql, params)


def fetchall(conn, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
    with get_cursor(conn) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def execute_batch(conn, sql: str, rows: Iterable[Sequence[Any]]) -> None:
    with get_cursor(conn) as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)


@contextmanager
def transaction(conn) -> Iterator[None]:
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise
