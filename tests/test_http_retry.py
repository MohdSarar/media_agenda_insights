from __future__ import annotations

import types
import requests

from core.http import fetch_url_text


class DummyResponse:
    def __init__(self, status_code: int, text: str = "OK"):
        self.status_code = status_code
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def test_fetch_url_text_retries_on_5xx_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def fake_get(self, url, headers=None, timeout=None):
        calls["n"] += 1
        # 2 échecs 503 puis succès 200
        if calls["n"] <= 2:
            return DummyResponse(503, "SERVICE UNAVAILABLE")
        return DummyResponse(200, "OK")

    session = requests.Session()
    monkeypatch.setattr(session, "get", types.MethodType(fake_get, session))

    out = fetch_url_text("https://example.com/feed", session=session)
    assert out == "OK"
    assert calls["n"] == 3


def test_fetch_url_text_retries_on_exception_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def fake_get(self, url, headers=None, timeout=None):
        calls["n"] += 1
        if calls["n"] <= 1:
            raise requests.Timeout("timeout")
        return DummyResponse(200, "OK")

    session = requests.Session()
    monkeypatch.setattr(session, "get", types.MethodType(fake_get, session))

    out = fetch_url_text("https://example.com/feed", session=session)
    assert out == "OK"
    assert calls["n"] == 2
