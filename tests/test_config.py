from __future__ import annotations

import os

import pytest

from cdr_pipeline.config import Config


@pytest.fixture(autouse=True)
def clear_config_env():
    keys = [
        "POSTGRES_PORT",
        "HTTP_RETRY_BACKOFF",
        "FETCH_PRODUCT_DETAILS",
        "PROVIDER_LIMIT",
        "MAX_PAGES_PER_PROVIDER",
    ]
    old = {k: os.environ.get(k) for k in keys}
    for k in keys:
        os.environ.pop(k, None)
    yield
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def test_config_defaults():
    cfg = Config.from_env()
    assert cfg.pg_port == 5432
    assert cfg.fetch_product_details is False
    assert cfg.provider_limit is None
    assert cfg.max_pages_per_provider == 200


def test_config_invalid_integer_raises():
    os.environ["POSTGRES_PORT"] = "not-a-number"
    with pytest.raises(ValueError, match="POSTGRES_PORT"):
        Config.from_env()


def test_config_invalid_bool_raises():
    os.environ["FETCH_PRODUCT_DETAILS"] = "sometimes"
    with pytest.raises(ValueError, match="FETCH_PRODUCT_DETAILS"):
        Config.from_env()
