from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from contextlib import closing
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv

from cdr_pipeline.bootstrap import bootstrap_db
from cdr_pipeline.config import Config
from cdr_pipeline.db import connect_with_retries, execute, execute_batch, transaction
from cdr_pipeline.drift import record_and_detect_drift
from cdr_pipeline.http_client import HttpRequestFailed, build_session, get_with_version_fallback

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _safe_filename(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in s)


def _write_bronze_json(run_date: str, provider_id: str, endpoint: str, page_num: int, payload_bytes: bytes) -> str:
    out_dir = os.path.join(
        "data",
        "bronze",
        f"ingestion_date={run_date}",
        f"provider={_safe_filename(provider_id)}",
        f"endpoint={_safe_filename(endpoint)}",
    )
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"page={page_num:04d}.json")
    with open(path, "wb") as f:
        f.write(payload_bytes)
    return path


def _resolve_next_url(current_url: str, next_url: str) -> str:
    if not next_url:
        return ""
    parsed = urlparse(next_url)
    if parsed.scheme and parsed.netloc:
        return next_url
    return urljoin(current_url, next_url)


def _insert_brand(conn, run_id: str, brand: dict, extracted_at: datetime) -> None:
    execute(
        conn,
        """
        INSERT INTO bronze.data_holder_brand (
            run_id, data_holder_brand_id, brand_name, brand_group, industries,
            public_base_uri, product_base_uri, logo_uri, last_updated, extracted_at
        ) VALUES (
            %s, %s, %s, %s, %s::jsonb,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (run_id, data_holder_brand_id) DO NOTHING
        """,
        (
            run_id,
            brand.get("dataHolderBrandId"),
            brand.get("brandName"),
            brand.get("brandGroup"),
            json.dumps(brand.get("industries", [])),
            brand.get("publicBaseUri"),
            brand.get("productBaseUri"),
            brand.get("logoUri"),
            brand.get("lastUpdated"),
            extracted_at,
        ),
    )


def _log_api_call(
    conn,
    run_id: str,
    provider_id: str,
    endpoint: str,
    url: str,
    status: int,
    responded_xv: int | None,
    fetched_at: datetime,
    etag: str | None,
    payload_hash: str | None,
    error: str | None,
) -> None:
    execute(
        conn,
        """
        INSERT INTO bronze.api_call_log (
            run_id, provider_id, endpoint, url, http_status, responded_xv, fetched_at, etag, payload_hash, error
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (run_id, provider_id, endpoint, url) DO UPDATE SET
            http_status = EXCLUDED.http_status,
            responded_xv = EXCLUDED.responded_xv,
            fetched_at = EXCLUDED.fetched_at,
            etag = EXCLUDED.etag,
            payload_hash = EXCLUDED.payload_hash,
            error = EXCLUDED.error
        """,
        (run_id, provider_id, endpoint, url, status, responded_xv, fetched_at, etag, payload_hash, error),
    )


def _insert_products_raw(
    conn,
    run_id: str,
    provider_id: str,
    brand_name: str | None,
    endpoint: str,
    url: str,
    page_num: int,
    status: int,
    responded_xv: int | None,
    fetched_at: datetime,
    etag: str | None,
    payload: Any | None,
    payload_hash: str | None,
) -> None:
    execute(
        conn,
        """
        INSERT INTO raw.products_raw (
            run_id, provider_id, brand_name, endpoint, url, page_num, http_status, responded_xv, fetched_at, etag, payload, payload_hash
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
        ON CONFLICT (run_id, provider_id, endpoint, page_num) DO UPDATE SET
            http_status = EXCLUDED.http_status,
            responded_xv = EXCLUDED.responded_xv,
            fetched_at = EXCLUDED.fetched_at,
            etag = EXCLUDED.etag,
            payload = EXCLUDED.payload,
            payload_hash = EXCLUDED.payload_hash
        """,
        (
            run_id,
            provider_id,
            brand_name,
            endpoint,
            url,
            page_num,
            status,
            responded_xv,
            fetched_at,
            etag,
            json.dumps(payload) if payload is not None else None,
            payload_hash,
        ),
    )


def _insert_product_detail_raw(
    conn,
    run_id: str,
    provider_id: str,
    brand_name: str | None,
    product_id: str,
    url: str,
    status: int,
    responded_xv: int | None,
    fetched_at: datetime,
    etag: str | None,
    payload: Any | None,
    payload_hash: str | None,
) -> None:
    execute(
        conn,
        """
        INSERT INTO raw.product_detail_raw (
            run_id, provider_id, brand_name, product_id, url, http_status, responded_xv, fetched_at, etag, payload, payload_hash
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
        ON CONFLICT (run_id, provider_id, product_id) DO UPDATE SET
            http_status = EXCLUDED.http_status,
            responded_xv = EXCLUDED.responded_xv,
            fetched_at = EXCLUDED.fetched_at,
            etag = EXCLUDED.etag,
            payload = EXCLUDED.payload,
            payload_hash = EXCLUDED.payload_hash
        """,
        (
            run_id,
            provider_id,
            brand_name,
            product_id,
            url,
            status,
            responded_xv,
            fetched_at,
            etag,
            json.dumps(payload) if payload is not None else None,
            payload_hash,
        ),
    )


def _discover_brands(cfg: Config, session, conn, run_id: str, run_date: str) -> list[dict]:
    url = f"{cfg.register_base}/cdr-register/v1/{cfg.register_industry}/data-holders/brands/summary"
    endpoint = "cdr-register:brands-summary"

    try:
        resp, responded_xv = get_with_version_fallback(
            session=session,
            url=url,
            timeout_seconds=cfg.timeout_seconds,
            preferred_xv=cfg.register_xv,
            fallback_versions=cfg.register_xv_fallback,
        )
    except HttpRequestFailed as e:
        fetched_at = datetime.utcnow()
        _log_api_call(
            conn,
            run_id,
            "cdr-register",
            endpoint,
            url,
            0,
            None,
            fetched_at,
            None,
            None,
            str(e),
        )
        raise

    payload_bytes = resp.content or b""
    fetched_at = datetime.utcnow()
    payload_hash = _sha256_bytes(payload_bytes) if payload_bytes else None
    _log_api_call(
        conn,
        run_id,
        "cdr-register",
        endpoint,
        url,
        resp.status_code,
        responded_xv,
        fetched_at,
        resp.headers.get("etag"),
        payload_hash,
        None if resp.status_code == 200 else (resp.text[:500] if resp.text else f"HTTP {resp.status_code}"),
    )
    _write_bronze_json(run_date, "cdr-register", endpoint, 1, payload_bytes)

    if resp.status_code != 200:
        raise RuntimeError(f"Register discovery failed: HTTP {resp.status_code} {resp.text[:300]}")

    data = (resp.json() or {}).get("data", [])
    filt = cfg.filter_industry.lower().strip()

    out = []
    for b in data:
        inds = [str(x).lower() for x in (b.get("industries") or [])]
        if filt in inds:
            out.append(b)
    return out


def _fetch_products_for_brand(cfg: Config, session, conn, run_id: str, run_date: str, brand: dict) -> tuple[int, set[str]]:
    provider_id = brand.get("dataHolderBrandId")
    brand_name = brand.get("brandName")
    base_uri = brand.get("productBaseUri") or brand.get("publicBaseUri")
    endpoint = "banking:get-products"

    if not provider_id or not base_uri:
        logger.warning("Skipping brand with missing provider_id/base_uri: %s", brand)
        return 0, set()

    products_url = urljoin(base_uri.rstrip("/") + "/", cfg.products_path.lstrip("/"))
    next_url = products_url
    seen_urls: set[str] = set()
    page_num = 1
    total_products = 0
    product_ids: set[str] = set()

    while next_url:
        if page_num > cfg.max_pages_per_provider:
            logger.error("Stopping pagination for provider %s after %s pages (MAX_PAGES_PER_PROVIDER).", provider_id, cfg.max_pages_per_provider)
            _log_api_call(
                conn,
                run_id,
                provider_id,
                endpoint,
                next_url,
                0,
                None,
                datetime.utcnow(),
                None,
                None,
                f"Pagination limit exceeded ({cfg.max_pages_per_provider})",
            )
            break

        if next_url in seen_urls:
            logger.error("Detected pagination loop for provider %s at URL %s. Stopping.", provider_id, next_url)
            _log_api_call(
                conn,
                run_id,
                provider_id,
                endpoint,
                next_url,
                0,
                None,
                datetime.utcnow(),
                None,
                None,
                "Pagination loop detected from links.next",
            )
            break
        seen_urls.add(next_url)

        fetched_at = datetime.utcnow()
        try:
            resp, responded_xv = get_with_version_fallback(
                session=session,
                url=next_url,
                timeout_seconds=cfg.timeout_seconds,
                preferred_xv=cfg.products_xv,
                fallback_versions=cfg.products_xv_fallback,
            )
        except HttpRequestFailed as e:
            _log_api_call(
                conn,
                run_id,
                provider_id,
                endpoint,
                next_url,
                0,
                None,
                fetched_at,
                None,
                None,
                str(e),
            )
            break
        etag = resp.headers.get("etag")
        status = resp.status_code
        payload_bytes = resp.content or b""
        payload_hash = _sha256_bytes(payload_bytes) if payload_bytes else None

        _log_api_call(
            conn,
            run_id,
            provider_id,
            endpoint,
            next_url,
            status,
            responded_xv,
            fetched_at,
            etag,
            payload_hash,
            None if status == 200 else (resp.text[:500] if resp.text else f"HTTP {status}"),
        )

        _write_bronze_json(run_date, provider_id, endpoint, page_num, payload_bytes)

        payload_obj = None
        if status == 200 and payload_bytes:
            try:
                payload_obj = resp.json()
            except Exception as e:  # noqa: BLE001
                payload_obj = None
                _log_api_call(conn, run_id, provider_id, endpoint, next_url, status, responded_xv, fetched_at, etag, payload_hash, f"JSON parse error: {e}")

        _insert_products_raw(conn, run_id, provider_id, brand_name, endpoint, next_url, page_num, status, responded_xv, fetched_at, etag, payload_obj, payload_hash)

        if payload_obj is not None:
            record_and_detect_drift(conn, provider_id, endpoint, payload_obj, fetched_at, run_id)
            products = (((payload_obj.get("data") or {}).get("products")) or [])
            total_products += len(products)
            for p in products:
                pid = p.get("productId")
                if pid:
                    product_ids.add(str(pid))

            links = payload_obj.get("links") or {}
            nxt = links.get("next") or ""
            next_url = _resolve_next_url(next_url, nxt)
            page_num += 1
        else:
            break

    return total_products, product_ids


def _fetch_product_details(cfg: Config, session, conn, run_id: str, run_date: str, brand: dict, product_ids: set[str]) -> int:
    provider_id = brand.get("dataHolderBrandId")
    brand_name = brand.get("brandName")
    base_uri = brand.get("productBaseUri") or brand.get("publicBaseUri")
    endpoint = "banking:get-product-detail"

    if not provider_id or not base_uri or not product_ids:
        return 0

    ok = 0
    for i, pid in enumerate(sorted(product_ids), start=1):
        url = urljoin(base_uri.rstrip("/") + "/", cfg.product_detail_path.lstrip("/").format(productId=pid))
        fetched_at = datetime.utcnow()
        try:
            resp, responded_xv = get_with_version_fallback(
                session=session,
                url=url,
                timeout_seconds=cfg.timeout_seconds,
                preferred_xv=cfg.product_detail_xv,
                fallback_versions=cfg.product_detail_xv_fallback,
            )
        except HttpRequestFailed as e:
            _log_api_call(
                conn,
                run_id,
                provider_id,
                endpoint,
                url,
                0,
                None,
                fetched_at,
                None,
                None,
                str(e),
            )
            continue
        etag = resp.headers.get("etag")
        status = resp.status_code
        payload_bytes = resp.content or b""
        payload_hash = _sha256_bytes(payload_bytes) if payload_bytes else None

        _log_api_call(
            conn,
            run_id,
            provider_id,
            endpoint,
            url,
            status,
            responded_xv,
            fetched_at,
            etag,
            payload_hash,
            None if status == 200 else (resp.text[:500] if resp.text else f"HTTP {status}"),
        )

        payload_obj = None
        if status == 200 and payload_bytes:
            try:
                payload_obj = resp.json()
            except Exception as e:  # noqa: BLE001
                payload_obj = None
                _log_api_call(conn, run_id, provider_id, endpoint, url, status, responded_xv, fetched_at, etag, payload_hash, f"JSON parse error: {e}")

        _insert_product_detail_raw(conn, run_id, provider_id, brand_name, pid, url, status, responded_xv, fetched_at, etag, payload_obj, payload_hash)

        if payload_obj is not None:
            record_and_detect_drift(conn, provider_id, endpoint, payload_obj, fetched_at, run_id)

        if status == 200:
            ok += 1

        if i % 50 == 0:
            logger.info("  detail progress: %s/%s", i, len(product_ids))

    # also persist raw details as bronze files (optional; one file per product might be large)
    # (kept in db only by default)

    return ok


def run_ingest(run_dt: datetime, provider_limit: int | None = None) -> None:
    load_dotenv(override=False)
    cfg = Config.from_env()

    bootstrap_db(force=False)
    with closing(connect_with_retries(cfg.pg_dsn(), autocommit=False)) as conn, closing(
        build_session(cfg.retry_total, cfg.retry_backoff, cfg.user_agent)
    ) as session:
        run_id = str(uuid.uuid4())
        run_date = run_dt.strftime("%Y-%m-%d")
        started_at = datetime.utcnow()

        with transaction(conn):
            execute(
                conn,
                """
                INSERT INTO bronze.pipeline_run (run_id, run_started_at, run_date, register_industry, filter_industry, fetch_product_details, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (run_id, started_at, run_date, cfg.register_industry, cfg.filter_industry, cfg.fetch_product_details, None),
            )

        # Discovery is isolated so its API-call diagnostics remain committed even if it fails.
        with transaction(conn):
            brands = _discover_brands(cfg, session, conn, run_id, run_date)
            logger.info("Discovered %s brands (filtered to industry=%s).", len(brands), cfg.filter_industry)

            extracted_at = datetime.utcnow()
            brand_rows = [
                (
                    run_id,
                    b.get("dataHolderBrandId"),
                    b.get("brandName"),
                    b.get("brandGroup"),
                    json.dumps(b.get("industries", [])),
                    b.get("publicBaseUri"),
                    b.get("productBaseUri"),
                    b.get("logoUri"),
                    b.get("lastUpdated"),
                    extracted_at,
                )
                for b in brands
            ]
            if brand_rows:
                execute_batch(
                    conn,
                    """
                    INSERT INTO bronze.data_holder_brand (
                        run_id, data_holder_brand_id, brand_name, brand_group, industries,
                        public_base_uri, product_base_uri, logo_uri, last_updated, extracted_at
                    ) VALUES (
                        %s, %s, %s, %s, %s::jsonb,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (run_id, data_holder_brand_id) DO NOTHING
                    """,
                    brand_rows,
                )

        limit = provider_limit if provider_limit is not None else cfg.provider_limit
        if limit and limit > 0:
            brands = brands[:limit]
            logger.info("Provider limit applied: processing %s brands.", len(brands))

        total_products = 0
        for idx, b in enumerate(brands, start=1):
            pid = b.get("dataHolderBrandId")
            name = b.get("brandName")
            logger.info("[%s/%s] Fetching products for %s (%s)...", idx, len(brands), name, pid)
            try:
                with transaction(conn):
                    n, product_ids = _fetch_products_for_brand(cfg, session, conn, run_id, run_date, b)
                    total_products += n
                    logger.info("  -> %s products ingested (sum of pages).", n)
                    if cfg.fetch_product_details:
                        ok = _fetch_product_details(cfg, session, conn, run_id, run_date, b, product_ids)
                        logger.info("  -> %s product details ingested.", ok)
            except Exception as e:  # noqa: BLE001
                logger.exception("Failed brand %s (%s): %s", name, pid, e)
                continue

        logger.info("Ingest complete. Total products (summed pages): %s. Run ID: %s", total_products, run_id)
