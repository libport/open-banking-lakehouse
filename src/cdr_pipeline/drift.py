from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from cdr_pipeline.db import fetchall, execute


def _extract_paths(obj: Any, prefix: str = "", max_depth: int = 4) -> set[str]:
    paths: set[str] = set()

    def rec(x: Any, p: str, depth: int) -> None:
        if depth > max_depth:
            return
        if isinstance(x, dict):
            for k, v in x.items():
                np = f"{p}.{k}" if p else k
                paths.add(np)
                rec(v, np, depth + 1)
        elif isinstance(x, list):
            np = f"{p}[]" if p else "[]"
            paths.add(np)
            for v in x[:3]:
                rec(v, np, depth + 1)

    rec(obj, prefix, 0)
    return paths


def fingerprint_payload(payload: Any, max_depth: int = 4) -> tuple[str, list[str]]:
    paths = sorted(_extract_paths(payload, max_depth=max_depth))
    h = hashlib.sha256(("\n".join(paths)).encode("utf-8")).hexdigest()
    return h, paths


def record_and_detect_drift(conn, provider_id: str, endpoint: str, payload: Any, observed_at: datetime, run_id: str) -> None:
    new_hash, paths = fingerprint_payload(payload)

    rows = fetchall(
        conn,
        """
        SELECT fingerprint_hash
        FROM bronze.schema_fingerprint
        WHERE provider_id = %s AND endpoint = %s
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        (provider_id, endpoint),
    )
    old_hash = rows[0][0] if rows else None

    execute(
        conn,
        """
        INSERT INTO bronze.schema_fingerprint (provider_id, endpoint, fingerprint_hash, fingerprint_paths, observed_at, run_id)
        VALUES (%s, %s, %s, %s::jsonb, %s, %s)
        """,
        (provider_id, endpoint, new_hash, json.dumps(paths), observed_at, run_id),
    )

    if old_hash and old_hash != new_hash:
        execute(
            conn,
            """
            INSERT INTO bronze.schema_drift_event (provider_id, endpoint, old_fingerprint_hash, new_fingerprint_hash, observed_at, run_id, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (provider_id, endpoint, old_hash, new_hash, observed_at, run_id, "Schema/path fingerprint changed"),
        )
