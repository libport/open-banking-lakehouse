from __future__ import annotations

import logging
from typing import Iterable, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def build_session(retry_total: int, backoff_factor: float, user_agent: str) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retry_total,
        connect=retry_total,
        read=retry_total,
        status=retry_total,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"Accept": "application/json", "User-Agent": user_agent})
    return session


def get_with_version_fallback(
    session: requests.Session,
    url: str,
    timeout_seconds: int,
    preferred_xv: int,
    fallback_versions: Iterable[int] = (),
    extra_headers: dict[str, str] | None = None,
) -> Tuple[requests.Response, int]:
    versions = [preferred_xv] + [v for v in fallback_versions if v != preferred_xv]
    headers = {}
    if extra_headers:
        headers.update(extra_headers)

    last_resp: requests.Response | None = None
    for xv in versions:
        headers["x-v"] = str(xv)
        resp = session.get(url, headers=headers, timeout=timeout_seconds)
        last_resp = resp
        if resp.status_code != 406:
            responded = resp.headers.get("x-v")
            try:
                return resp, int(responded) if responded else xv
            except ValueError:
                return resp, xv
        logger.warning("406 for %s with x-v=%s; trying fallback...", url, xv)

    assert last_resp is not None
    responded = last_resp.headers.get("x-v")
    try:
        return last_resp, int(responded) if responded else preferred_xv
    except ValueError:
        return last_resp, preferred_xv
