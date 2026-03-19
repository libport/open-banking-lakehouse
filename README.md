# AU Open Banking (CDR) Product & Pricing Lakehouse

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-required-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![dbt 1.9](https://img.shields.io/badge/dbt-1.9-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Postgres 16](https://img.shields.io/badge/postgres-16-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Tests: pytest](https://img.shields.io/badge/tests-pytest-0A9EDC?logo=pytest&logoColor=white)](https://docs.pytest.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Local-first data pipeline for Australian Consumer Data Right (CDR) Open Banking public product APIs. It discovers registered data holder brands, ingests public product and pricing payloads, stores raw JSON locally and in Postgres, transforms them with dbt, and produces daily reporting and QA artifacts.

## What It Does

- Discovers Data Holder brands from the CDR Register brands summary endpoint
- Ingests unauthenticated public product payloads from each provider
- Stores raw payloads in both local `data/bronze/` partitions and Postgres JSONB tables
- Detects schema drift and records drift events per provider and endpoint
- Builds staging, silver, and gold marts with dbt
- Generates daily rate-change and provider-coverage reports
- Runs configurable QA gates, with optional `dbt test` execution
- Supports local Metabase for ad hoc exploration

Notes:
- Public product endpoints are unauthenticated by design, but providers may rate limit or have intermittent availability issues.
- API versions vary across providers. The HTTP client includes version fallback when a preferred `x-v` is rejected.
- Pagination safety includes loop detection and a maximum pages-per-provider cap.

## Architecture

```text
CDR Register + provider product APIs
  -> Python ingestion pipeline
  -> local bronze JSON files + Postgres bronze/raw tables
  -> dbt staging/silver/gold models
  -> CSV + Markdown reports
  -> QA gate results
  -> optional Metabase queries/dashboarding
```

## Repo Layout

```text
src/cdr_pipeline/   Python CLI, ingestion, bootstrap, reporting, QA
dbt/                dbt project, models, macros, profiles
tests/              pytest coverage for config, reporting, QA, regressions
Dockerfile          pipeline image
docker-compose.yml  Postgres, dbt, Metabase, pipeline services
Makefile            common local and Docker workflows
```

## Quickstart

Requirements:
- Docker
- Docker Compose

Run the full pipeline locally with containers:

```bash
# Start Postgres and Metabase
make up

# Ingest -> dbt build -> report
make run

# Run QA gates (dbt tests skipped in this Make target)
make qa
```

Service endpoints:
- Metabase: http://localhost:3000
- Postgres: `localhost:5432`
- Default Postgres database/user/password: `cdr`

If Metabase image startup is the only issue, you can still run the data pipeline against Postgres alone:

```bash
docker compose up -d postgres
```

## CLI Commands

The package exposes a `cdr-pipeline` CLI after editable install, and the same commands are available through `python -m cdr_pipeline`.

Available subcommands:
- `bootstrap`: create required schemas and tables
- `ingest`: discover brands and ingest raw product payloads
- `report`: generate CSV and Markdown report artifacts
- `qa`: run quality gates and optionally execute `dbt test`

Examples:

```bash
python -m cdr_pipeline bootstrap
python -m cdr_pipeline ingest --date 2026-02-10
python -m cdr_pipeline report --date 2026-02-10
python -m cdr_pipeline qa --date 2026-02-10 --skip-dbt-tests
```

## Local Development

Use this path when you want to run the Python pipeline on your host instead of inside the `pipeline` container.

Requirements:
- Python 3.10+
- A running Postgres instance
- dbt if you want to run dbt outside Docker

The simplest setup is to reuse the repo's Postgres container:

```bash
make up

python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

Run the pipeline locally:

```bash
cdr-pipeline ingest --date 2026-02-10

# Option A: dbt in Docker
docker compose run --rm dbt dbt build

# Option B: dbt installed locally
# dbt build --project-dir dbt --profiles-dir dbt

cdr-pipeline report --date 2026-02-10
cdr-pipeline qa --date 2026-02-10 --skip-dbt-tests
```

Equivalent module entry points:

```bash
python -m cdr_pipeline ingest --date 2026-02-10
python -m cdr_pipeline report --date 2026-02-10
python -m cdr_pipeline qa --date 2026-02-10 --skip-dbt-tests
```

## Make Targets

```bash
make up
make down
make ingest
make dbt
make report
make qa
make run
make logs-postgres
make logs-metabase
```

Override the run date when using Make:

```bash
DATE=2026-02-10 make run
```

## Data And Outputs

Storage created by the pipeline:
- Local raw files under `data/bronze/ingestion_date=<date>/provider=<id>/endpoint=<name>/page=<n>.json`
- Postgres schema `bronze` for run metadata, brands, API call logs, schema fingerprints, drift events, and QA gate results
- Postgres schema `raw` for raw product and product-detail payloads
- dbt output schemas typically materialized as `public_staging`, `public_silver`, and `public_gold` with the current [`dbt/profiles.yml`](dbt/profiles.yml)

Report artifacts written under `reports/`:
- `rate_changes_<YYYY-MM-DD>.csv`
- `provider_coverage_<YYYY-MM-DD>.csv`
- `pipeline_summary_<YYYY-MM-DD>.md`
- `qa_summary_<YYYY-MM-DD>.md`

## Configuration

The code loads environment variables with `python-dotenv`, so you can define them in a local `.env` file if you want. There is currently no committed `.env.example`; Docker defaults are enough for a local first run.

Core connection settings:
- `POSTGRES_HOST` default `localhost`
- `POSTGRES_PORT` default `5432`
- `POSTGRES_DB` default `cdr`
- `POSTGRES_USER` default `cdr`
- `POSTGRES_PASSWORD` default `cdr`

CDR register and product API settings:
- `CDR_REGISTER_BASE` default `https://api.cdr.gov.au`
- `CDR_REGISTER_INDUSTRY` default `all`
- `CDR_FILTER_INDUSTRY` default `banking`
- `CDR_REGISTER_XV` default `2`
- `CDR_REGISTER_XV_FALLBACK` default `1`
- `CDR_PRODUCTS_PATH` default `/cds-au/v1/banking/products`
- `CDR_PRODUCTS_XV` default `4`
- `CDR_PRODUCTS_XV_FALLBACK` default `3,2,1`
- `CDR_PRODUCT_DETAIL_PATH` default `/cds-au/v1/banking/products/{productId}`
- `CDR_PRODUCT_DETAIL_XV` default `6`
- `CDR_PRODUCT_DETAIL_XV_FALLBACK` default `5,4,3,2,1`
- `FETCH_PRODUCT_DETAILS` default `false`
- `PROVIDER_LIMIT` optional integer limit for quick runs

HTTP and safety settings:
- `HTTP_TIMEOUT_SECONDS` default `30`
- `HTTP_RETRY_TOTAL` default `5`
- `HTTP_RETRY_BACKOFF` default `0.4`
- `HTTP_USER_AGENT` default `cdr-open-banking-lakehouse-local/1.0`
- `MAX_PAGES_PER_PROVIDER` default `200`

QA settings:
- `QA_MIN_PROVIDERS_OK` default `1`
- `QA_MIN_PRODUCTS` default `1`
- `QA_MIN_RATE_CHANGES` default `1`
- `QA_MAX_FRESHNESS_HOURS` default `36`
- `QA_FAIL_ON_SCHEMA_DRIFT` default `false`
- `QA_RUN_DBT_TESTS` default `true`
- `QA_DBT_TEST_COMMAND` default `dbt test --project-dir dbt --profiles-dir dbt`

## Verification

After local install, run:

```bash
ruff check src tests
pytest -q
```

If you want to run tests without editable install, make the package importable first:

```bash
PYTHONPATH=src pytest -q
```

You still need the project dependencies installed for that to work.

## License

MIT. See [LICENSE](LICENSE).
