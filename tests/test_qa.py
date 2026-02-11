from __future__ import annotations

from cdr_pipeline.qa import _clip_text, _gate_max, _gate_min, _run_dbt_tests


def test_clip_text_short_no_change():
    assert _clip_text("abc", max_chars=5) == "abc"


def test_clip_text_truncates():
    assert _clip_text("abcdef", max_chars=5) == "ab..."


def test_gate_min_pass_and_fail():
    assert _gate_min("x", 10, 5).passed is True
    assert _gate_min("x", 2, 5).passed is False


def test_gate_max_pass_and_fail():
    assert _gate_max("x", 2, 5).passed is True
    assert _gate_max("x", 10, 5).passed is False


def test_run_dbt_tests_empty_command_fails():
    ok, details = _run_dbt_tests("")
    assert ok is False
    assert "empty" in details
