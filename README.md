# AU Open Banking (CDR) Product & Pricing Lakehouse - Local-Run - Work in Progress

This project builds a local-first data engineering pipeline for Australian financial services using Consumer Data Right (CDR) Open Banking public product APIs:
- Discovers Data Holder brands via the CDR Register "Get Data Holder Brands Summary" endpoint
- Ingests public product & pricing payloads from each Data Holder (unauthenticated)
- Stores raw JSON ("bronze") to local disk + "raw" JSONB tables in Postgres
- Transforms to analytics-ready tables with dbt (staging → silver → gold)
- Produces a daily rate-change report (CSV + Markdown)
- Runs configurable QA gates with optional dbt test execution
- Optional: view dashboards in Metabase (local)

> Notes:
> - Public product endpoints are unauthenticated by design, but Data Holders may apply rate limits or availability constraints.
> - API versions can vary across Data Holders. This pipeline includes version fallback when it receives a 406.
> - Ingestion includes pagination loop detection and a configurable page cap per provider for safety.

## Quickstart (Docker-only)

Requirements:
- Docker + Docker Compose

```bash
# 1) Start Postgres + Metabase
make up

# 2) Run a full pipeline run (ingest → dbt build → report)
make run

# 3) Run QA gates (in Docker, dbt tests are skipped)
make qa
```

If `make up` fails due a Metabase image tag issue, start only Postgres:

```bash
docker compose up -d postgres
```

Metabase will be available at:
- http://localhost:3000

Postgres is available at:
- localhost:5432 (db/user/pass all `cdr` by default)

## Local development (optional, run pipeline on your host)

If you want to run the **Python pipeline outside Docker** (for faster iteration / debugging), the repo now includes a `pyproject.toml` so you can install it in editable mode and use the `cdr-pipeline` CLI.

Requirements:
- Python 3.10+
- A Postgres instance (easiest: start the project's containerized Postgres with `make up`)
- dbt (either use the provided `dbt` Docker service, or install `dbt-postgres` locally)

```bash
# 0) Start Postgres (+ Metabase if you want it)
make up

# 1) Create a virtualenv and install the package (reads deps from requirements.txt)
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .

# 2) Run ingest → dbt build → report
cdr-pipeline ingest --date 2026-02-10

# Option A: run dbt via Docker (recommended if you don't want local dbt)
docker compose run --rm dbt build

# Option B: run dbt locally (if you installed dbt-postgres)
# dbt build --project-dir dbt

cdr-pipeline report --date 2026-02-10

# 3) Run QA gates + dbt tests (local dbt installed)
cdr-pipeline qa --date 2026-02-10

# If you use dbt via Docker, run dbt test separately and skip inside QA
docker compose run --rm dbt test
cdr-pipeline qa --date 2026-02-10 --skip-dbt-tests
```

You can also call the module directly (equivalent to the CLI):

```bash
python -m cdr_pipeline ingest --date 2026-02-10
python -m cdr_pipeline report --date 2026-02-10
python -m cdr_pipeline qa --date 2026-02-10 --skip-dbt-tests
```

## What gets created

### Storage
- Local raw files: `data/bronze/...` (partitioned by date/provider/endpoint/page)
- Postgres schemas:
  - `bronze` - pipeline run metadata, API call logs, drift events, discovered brands, QA gate results
  - `raw` - raw API payloads stored as JSONB
  - dbt output schemas default to `public_staging`, `public_silver`, `public_gold` with current `dbt/profiles.yml`

### Outputs
- `reports/`:
  - `rate_changes_<YYYY-MM-DD>.csv`
  - `pipeline_summary_<YYYY-MM-DD>.md`
  - `qa_summary_<YYYY-MM-DD>.md`

## Useful commands

```bash
make up
make down

# Ingest only
make ingest

# Transform only
make dbt

# Report only
make report

# QA gates (skip dbt tests in container)
make qa
```

Basic local quality checks:

```bash
pip install -e ".[dev]"
ruff check src tests
pytest -q
```

You can override the run date:

```bash
DATE=2026-02-10 make run
```

## Configuration

Copy `.env.example` to `.env` (optional). Defaults work out-of-the-box for local Docker.

Key env vars:
- `CDR_REGISTER_INDUSTRY` (default: `all`) - discovery endpoint industry path
- `CDR_FILTER_INDUSTRY` (default: `banking`) - keep brands that support this industry
- `CDR_PRODUCTS_XV` (default: `4`) - preferred x-v for Get Products (fallbacks included)
- `CDR_REGISTER_XV` (default: `2`) - preferred x-v for Brands Summary (fallbacks included)
- `FETCH_PRODUCT_DETAILS` (default: `false`) - if true, also calls Get Product Detail for each productId
- `PROVIDER_LIMIT` (default: empty) - set to an integer to limit number of providers (useful for quick runs)
- `MAX_PAGES_PER_PROVIDER` (default: `200`) - hard cap to prevent pagination loops or runaway fetches
- `QA_MIN_PROVIDERS_OK` (default: `1`) - minimum providers with successful product fetch in `gold.mart_provider_coverage`
- `QA_MIN_PRODUCTS` (default: `1`) - minimum rows in `silver.dim_products` for QA date
- `QA_MIN_RATE_CHANGES` (default: `1`) - minimum rows in `gold.mart_rate_changes` for QA date
- `QA_MAX_FRESHNESS_HOURS` (default: `36`) - max age (hours) of latest `raw.products_raw.fetched_at`
- `QA_FAIL_ON_SCHEMA_DRIFT` (default: `false`) - fail QA when any `bronze.schema_drift_event` occurs on QA date
- `QA_RUN_DBT_TESTS` (default: `true`) - run dbt tests as part of `cdr-pipeline qa`
- `QA_DBT_TEST_COMMAND` (default: `dbt test --project-dir dbt --profiles-dir dbt`) - command used for dbt test execution

## License
MIT (see `LICENSE`).
