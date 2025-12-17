from __future__ import annotations

import types
import requests

from core.http import fetch_json


class DummyResponse:
    def __init__(self, status_code: int, payload=None):
        self.status_code = status_code
        self._payload = payload or {"ok": True}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def test_fetch_json_retries_on_429_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def fake_get(self, url, params=None, headers=None, timeout=None):
        calls["n"] += 1
        if calls["n"] <= 2:
            return DummyResponse(429, {"error": "rate limited"})
        return DummyResponse(200, {"data": "ok"})

    session = requests.Session()
    monkeypatch.setattr(session, "get", types.MethodType(fake_get, session))

    out = fetch_json("https://www.reddit.com/r/test/new.json", params={"limit": 1}, headers={"User-Agent": "X"}, session=session)
    assert out == {"data": "ok"}
    assert calls["n"] == 3
