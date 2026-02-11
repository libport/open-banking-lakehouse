# AU Open Banking (CDR) Product & Pricing Lakehouse - Local-Run - Work in Progress

This project builds a local-first data engineering pipeline for Australian financial services using Consumer Data Right (CDR) Open Banking public product APIs:
- Discovers Data Holder brands via the CDR Register "Get Data Holder Brands Summary" endpoint
- Ingests public product & pricing payloads from each Data Holder (unauthenticated)
- Stores raw JSON ("bronze") to local disk + "raw" JSONB tables in Postgres
- Transforms to analytics-ready tables with dbt (staging → silver → gold)
- Produces a daily rate-change report (CSV + Markdown)
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
docker compose run --rm dbt dbt build

# Option B: run dbt locally (if you installed dbt-postgres)
# dbt build --project-dir dbt

cdr-pipeline report --date 2026-02-10
```

You can also call the module directly (equivalent to the CLI):

```bash
python -m cdr_pipeline ingest --date 2026-02-10
python -m cdr_pipeline report --date 2026-02-10
```

## What gets created

### Storage
- Local raw files: `data/bronze/...` (partitioned by date/provider/endpoint/page)
- Postgres schemas:
  - `bronze` - pipeline run metadata, API call logs, drift events, discovered brands
  - `raw` - raw API payloads stored as JSONB
  - `staging` - dbt staging views
  - `silver` - normalized, analysis-friendly tables
  - `gold` - marts (rate changes + pipeline coverage)

### Outputs
- `reports/`:
  - `rate_changes_<YYYY-MM-DD>.csv`
  - `pipeline_summary_<YYYY-MM-DD>.md`

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

## License
MIT (see `LICENSE`).
