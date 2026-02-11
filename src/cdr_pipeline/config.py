from __future__ import annotations

import os
from dataclasses import dataclass


def _getenv(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return val


def _parse_csv_ints(s: str | None) -> list[int]:
    if not s:
        return []
    out: list[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return out


def _require_int(name: str, default: str) -> int:
    raw = _getenv(name, default)
    assert raw is not None
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"Environment variable {name} must be an integer, got: {raw!r}") from e


def _require_float(name: str, default: str) -> float:
    raw = _getenv(name, default)
    assert raw is not None
    try:
        return float(raw)
    except ValueError as e:
        raise ValueError(f"Environment variable {name} must be a float, got: {raw!r}") from e


def _parse_optional_int(name: str) -> int | None:
    raw = _getenv(name, None)
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"Environment variable {name} must be an integer if set, got: {raw!r}") from e


def _parse_bool(name: str, default: str = "false") -> bool:
    raw = (_getenv(name, default) or default).strip().lower()
    if raw in ("1", "true", "yes", "y"):
        return True
    if raw in ("0", "false", "no", "n"):
        return False
    raise ValueError(f"Environment variable {name} must be a boolean value, got: {raw!r}")


@dataclass(frozen=True)
class Config:
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str

    register_base: str
    register_industry: str
    filter_industry: str
    register_xv: int
    register_xv_fallback: list[int]

    products_path: str
    products_xv: int
    products_xv_fallback: list[int]

    product_detail_path: str
    product_detail_xv: int
    product_detail_xv_fallback: list[int]

    timeout_seconds: int
    retry_total: int
    retry_backoff: float
    user_agent: str
    fetch_product_details: bool
    provider_limit: int | None
    max_pages_per_provider: int
    qa_min_providers_ok: int
    qa_min_products: int
    qa_min_rate_changes: int
    qa_max_freshness_hours: float
    qa_fail_on_schema_drift: bool
    qa_run_dbt_tests: bool
    qa_dbt_test_command: str

    @staticmethod
    def from_env() -> Config:
        provider_limit = _parse_optional_int("PROVIDER_LIMIT")

        return Config(
            pg_host=_getenv("POSTGRES_HOST", "localhost") or "localhost",
            pg_port=_require_int("POSTGRES_PORT", "5432"),
            pg_db=_getenv("POSTGRES_DB", "cdr") or "cdr",
            pg_user=_getenv("POSTGRES_USER", "cdr") or "cdr",
            pg_password=_getenv("POSTGRES_PASSWORD", "cdr") or "cdr",

            register_base=(_getenv("CDR_REGISTER_BASE", "https://api.cdr.gov.au") or "https://api.cdr.gov.au").rstrip("/"),
            register_industry=_getenv("CDR_REGISTER_INDUSTRY", "all") or "all",
            filter_industry=_getenv("CDR_FILTER_INDUSTRY", "banking") or "banking",
            register_xv=_require_int("CDR_REGISTER_XV", "2"),
            register_xv_fallback=_parse_csv_ints(_getenv("CDR_REGISTER_XV_FALLBACK", "1")),

            products_path=_getenv("CDR_PRODUCTS_PATH", "/cds-au/v1/banking/products") or "/cds-au/v1/banking/products",
            products_xv=_require_int("CDR_PRODUCTS_XV", "4"),
            products_xv_fallback=_parse_csv_ints(_getenv("CDR_PRODUCTS_XV_FALLBACK", "3,2,1")),

            product_detail_path=_getenv("CDR_PRODUCT_DETAIL_PATH", "/cds-au/v1/banking/products/{productId}") or "/cds-au/v1/banking/products/{productId}",
            product_detail_xv=_require_int("CDR_PRODUCT_DETAIL_XV", "6"),
            product_detail_xv_fallback=_parse_csv_ints(_getenv("CDR_PRODUCT_DETAIL_XV_FALLBACK", "5,4,3,2,1")),

            timeout_seconds=_require_int("HTTP_TIMEOUT_SECONDS", "30"),
            retry_total=_require_int("HTTP_RETRY_TOTAL", "5"),
            retry_backoff=_require_float("HTTP_RETRY_BACKOFF", "0.4"),
            user_agent=_getenv("HTTP_USER_AGENT", "cdr-open-banking-lakehouse-local/1.0") or "cdr-open-banking-lakehouse-local/1.0",
            fetch_product_details=_parse_bool("FETCH_PRODUCT_DETAILS", "false"),
            provider_limit=provider_limit,
            max_pages_per_provider=_require_int("MAX_PAGES_PER_PROVIDER", "200"),
            qa_min_providers_ok=_require_int("QA_MIN_PROVIDERS_OK", "1"),
            qa_min_products=_require_int("QA_MIN_PRODUCTS", "1"),
            qa_min_rate_changes=_require_int("QA_MIN_RATE_CHANGES", "1"),
            qa_max_freshness_hours=_require_float("QA_MAX_FRESHNESS_HOURS", "36"),
            qa_fail_on_schema_drift=_parse_bool("QA_FAIL_ON_SCHEMA_DRIFT", "false"),
            qa_run_dbt_tests=_parse_bool("QA_RUN_DBT_TESTS", "true"),
            qa_dbt_test_command=_getenv("QA_DBT_TEST_COMMAND", "dbt test --project-dir dbt --profiles-dir dbt")
            or "dbt test --project-dir dbt --profiles-dir dbt",
        )

    def pg_dsn(self) -> str:
        return f"dbname={self.pg_db} user={self.pg_user} password={self.pg_password} host={self.pg_host} port={self.pg_port}"
