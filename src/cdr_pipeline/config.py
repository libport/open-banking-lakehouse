from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List


def _getenv(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return val


def _parse_csv_ints(s: str | None) -> List[int]:
    if not s:
        return []
    out: List[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return out


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
    register_xv_fallback: List[int]

    products_path: str
    products_xv: int
    products_xv_fallback: List[int]

    product_detail_path: str
    product_detail_xv: int
    product_detail_xv_fallback: List[int]

    timeout_seconds: int
    retry_total: int
    retry_backoff: float
    user_agent: str
    fetch_product_details: bool
    provider_limit: int | None

    @staticmethod
    def from_env() -> "Config":
        provider_limit_raw = _getenv("PROVIDER_LIMIT", None)
        provider_limit = int(provider_limit_raw) if provider_limit_raw and provider_limit_raw.isdigit() else None

        return Config(
            pg_host=_getenv("POSTGRES_HOST", "localhost") or "localhost",
            pg_port=int(_getenv("POSTGRES_PORT", "5432") or "5432"),
            pg_db=_getenv("POSTGRES_DB", "cdr") or "cdr",
            pg_user=_getenv("POSTGRES_USER", "cdr") or "cdr",
            pg_password=_getenv("POSTGRES_PASSWORD", "cdr") or "cdr",

            register_base=(_getenv("CDR_REGISTER_BASE", "https://api.cdr.gov.au") or "https://api.cdr.gov.au").rstrip("/"),
            register_industry=_getenv("CDR_REGISTER_INDUSTRY", "all") or "all",
            filter_industry=_getenv("CDR_FILTER_INDUSTRY", "banking") or "banking",
            register_xv=int(_getenv("CDR_REGISTER_XV", "2") or "2"),
            register_xv_fallback=_parse_csv_ints(_getenv("CDR_REGISTER_XV_FALLBACK", "1")),

            products_path=_getenv("CDR_PRODUCTS_PATH", "/cds-au/v1/banking/products") or "/cds-au/v1/banking/products",
            products_xv=int(_getenv("CDR_PRODUCTS_XV", "4") or "4"),
            products_xv_fallback=_parse_csv_ints(_getenv("CDR_PRODUCTS_XV_FALLBACK", "3,2,1")),

            product_detail_path=_getenv("CDR_PRODUCT_DETAIL_PATH", "/cds-au/v1/banking/products/{productId}") or "/cds-au/v1/banking/products/{productId}",
            product_detail_xv=int(_getenv("CDR_PRODUCT_DETAIL_XV", "6") or "6"),
            product_detail_xv_fallback=_parse_csv_ints(_getenv("CDR_PRODUCT_DETAIL_XV_FALLBACK", "5,4,3,2,1")),

            timeout_seconds=int(_getenv("HTTP_TIMEOUT_SECONDS", "30") or "30"),
            retry_total=int(_getenv("HTTP_RETRY_TOTAL", "5") or "5"),
            retry_backoff=float(_getenv("HTTP_RETRY_BACKOFF", "0.4") or "0.4"),
            user_agent=_getenv("HTTP_USER_AGENT", "cdr-open-banking-lakehouse-local/1.0") or "cdr-open-banking-lakehouse-local/1.0",
            fetch_product_details=(_getenv("FETCH_PRODUCT_DETAILS", "false") or "false").lower() in ("1", "true", "yes", "y"),
            provider_limit=provider_limit,
        )

    def pg_dsn(self) -> str:
        return f"dbname={self.pg_db} user={self.pg_user} password={self.pg_password} host={self.pg_host} port={self.pg_port}"
