from __future__ import annotations

import logging

from cdr_pipeline.config import Config
from cdr_pipeline.db import connect_with_retries, execute

logger = logging.getLogger(__name__)

DDL = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS bronze.pipeline_run (
    run_id uuid PRIMARY KEY,
    run_started_at timestamptz NOT NULL,
    run_date date NOT NULL,
    register_industry text NOT NULL,
    filter_industry text NOT NULL,
    fetch_product_details boolean NOT NULL,
    notes text
);

CREATE TABLE IF NOT EXISTS bronze.data_holder_brand (
    run_id uuid NOT NULL REFERENCES bronze.pipeline_run(run_id),
    data_holder_brand_id text NOT NULL,
    brand_name text,
    brand_group text,
    industries jsonb,
    public_base_uri text,
    product_base_uri text,
    logo_uri text,
    last_updated text,
    extracted_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, data_holder_brand_id)
);

CREATE TABLE IF NOT EXISTS bronze.api_call_log (
    run_id uuid NOT NULL REFERENCES bronze.pipeline_run(run_id),
    provider_id text NOT NULL,
    endpoint text NOT NULL,
    url text NOT NULL,
    http_status int NOT NULL,
    responded_xv int,
    fetched_at timestamptz NOT NULL,
    etag text,
    payload_hash text,
    error text,
    PRIMARY KEY (run_id, provider_id, endpoint, url)
);

CREATE TABLE IF NOT EXISTS bronze.schema_fingerprint (
    provider_id text NOT NULL,
    endpoint text NOT NULL,
    fingerprint_hash text NOT NULL,
    fingerprint_paths jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    run_id uuid NOT NULL REFERENCES bronze.pipeline_run(run_id),
    PRIMARY KEY (provider_id, endpoint, observed_at)
);

CREATE TABLE IF NOT EXISTS bronze.schema_drift_event (
    provider_id text NOT NULL,
    endpoint text NOT NULL,
    old_fingerprint_hash text,
    new_fingerprint_hash text NOT NULL,
    observed_at timestamptz NOT NULL,
    run_id uuid NOT NULL REFERENCES bronze.pipeline_run(run_id),
    note text,
    PRIMARY KEY (provider_id, endpoint, observed_at)
);

CREATE TABLE IF NOT EXISTS raw.products_raw (
    run_id uuid NOT NULL REFERENCES bronze.pipeline_run(run_id),
    provider_id text NOT NULL,
    brand_name text,
    endpoint text NOT NULL,
    url text NOT NULL,
    page_num int NOT NULL,
    http_status int NOT NULL,
    responded_xv int,
    fetched_at timestamptz NOT NULL,
    etag text,
    payload jsonb,
    payload_hash text,
    PRIMARY KEY (run_id, provider_id, endpoint, page_num)
);

CREATE TABLE IF NOT EXISTS raw.product_detail_raw (
    run_id uuid NOT NULL REFERENCES bronze.pipeline_run(run_id),
    provider_id text NOT NULL,
    brand_name text,
    product_id text NOT NULL,
    url text NOT NULL,
    http_status int NOT NULL,
    responded_xv int,
    fetched_at timestamptz NOT NULL,
    etag text,
    payload jsonb,
    payload_hash text,
    PRIMARY KEY (run_id, provider_id, product_id)
);
"""


def bootstrap_db(force: bool = False) -> None:
    cfg = Config.from_env()
    conn = connect_with_retries(cfg.pg_dsn())

    if force:
        logger.warning("FORCE: dropping schemas (deletes all data).")
        execute(conn, "DROP SCHEMA IF EXISTS gold CASCADE;")
        execute(conn, "DROP SCHEMA IF EXISTS silver CASCADE;")
        execute(conn, "DROP SCHEMA IF EXISTS staging CASCADE;")
        execute(conn, "DROP SCHEMA IF EXISTS raw CASCADE;")
        execute(conn, "DROP SCHEMA IF EXISTS bronze CASCADE;")

    execute(conn, DDL)
    logger.info("Bootstrapped Postgres schemas/tables.")
