"""Unit tests for the authenticated operational smoke checks."""

import importlib.util
from pathlib import Path

import pytest


_SMOKE_TEST_PATH = Path(__file__).resolve().parents[1] / "tools" / "smoke_test.py"
_SPEC = importlib.util.spec_from_file_location("smoke_test", _SMOKE_TEST_PATH)
assert _SPEC and _SPEC.loader
smoke_test = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(smoke_test)


def test_run_rejects_map_summary_without_required_aggregate_fields(monkeypatch):
    def fake_request(_base_url, path, method="GET", token=None, body=None):
        if path == "/health":
            return 200, {"status": "healthy"}
        if path == "/health/ready":
            return 200, {"status": "ready"}
        if path == "/":
            return 200, '<script src="/static/assets/index.js"></script>'
        if path == "/static/assets/index.js":
            return 200, "asset"
        if path == "/api/login":
            return 200, {"status": "ok", "token": "test-token"}
        if path == "/api/processos/mapa/resumo?limit_cidades=200":
            assert token == "test-token"
            return 200, {"total_processos": 1, "items": []}
        return 200, {}

    monkeypatch.setattr(smoke_test, "_request", fake_request)

    with pytest.raises(AssertionError, match="mapa processos"):
        smoke_test.run("http://radar.test", "admin", "not-a-secret")
