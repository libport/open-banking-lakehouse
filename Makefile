SHELL := /bin/bash
DATE ?= $(shell date -u +%F)

up:
	docker compose up -d postgres metabase

down:
	docker compose down

ingest:
	docker compose run --rm pipeline python -m cdr_pipeline ingest --date $(DATE)

dbt:
	docker compose run --rm dbt dbt build

report:
	docker compose run --rm pipeline python -m cdr_pipeline report --date $(DATE)

qa:
	docker compose run --rm pipeline python -m cdr_pipeline qa --date $(DATE) --skip-dbt-tests

run: ingest dbt report

logs-postgres:
	docker compose logs -f postgres

logs-metabase:
	docker compose logs -f metabase
