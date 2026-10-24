"""Fail-fast PrizePicks HTTP must not sleep 60–120s on 429/403."""
from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_pp_api():
    import importlib.util

    os.environ.setdefault("PROPORACLE_CURL_IMPERSONATE", "chrome131")
    path = REPO_ROOT / "Sports" / "NBA" / "scripts" / "step1_fetch_prizepicks_api.py"
    spec = importlib.util.spec_from_file_location("pp_api_fail_fast_429", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Resp:
    def __init__(self, status_code: int, payload=None):
        self.status_code = status_code
        self._payload = payload or {"data": [], "included": []}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_fail_fast_429_does_not_sleep(monkeypatch):
    mod = _load_pp_api()
    sleeps: list[float] = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))
    session = SimpleNamespace(get=lambda *a, **k: _Resp(429))
    with pytest.raises(RuntimeError, match="HTTP_429_FAIL_FAST"):
        mod._api_get(session, mod.BASE_URL, {"league_id": "241"}, retries=2, fail_fast=True)
    assert sleeps == []


def test_zero_cooldown_windows_treats_429_as_fail_fast(monkeypatch):
    mod = _load_pp_api()
    sleeps: list[float] = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))
    session = SimpleNamespace(get=lambda *a, **k: _Resp(429))
    with pytest.raises(RuntimeError, match="HTTP_429_FAIL_FAST"):
        mod._api_get(
            session,
            mod.BASE_URL,
            {"league_id": "241"},
            retries=2,
            forbid_max_cooldown_windows=0,
        )
    assert sleeps == []


def test_partner_empty_board_does_not_hit_public_api(monkeypatch):
    mod = _load_pp_api()
    urls: list[str] = []

    def fake_get(session, url, params, **kw):
        urls.append(url)
        return {"data": [], "included": []}

    monkeypatch.setattr(mod, "_api_get", fake_get)
    monkeypatch.setattr(mod, "_make_session", lambda **k: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    data, included = mod.fetch_projections(
        "241",
        retries=1,
        first_page_waves=1,
        fail_fast=True,
        session_jitter=(0.0, 0.0),
        inter_page_delay=(0.0, 0.0),
    )
    assert data == []
    assert included == []
    assert urls
    assert all("partner-api.prizepicks.com" in u for u in urls)
    assert not any(u.startswith("https://api.prizepicks.com") for u in urls)
