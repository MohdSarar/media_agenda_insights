from __future__ import annotations

import time
import datetime as dt

from ingestion.tv.ingest_tv import parse_entry


class Entry(dict):
    """feedparser entries are dict-like + attribute access."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as e:
            raise AttributeError(name) from e


def test_parse_entry_returns_none_when_no_link():
    e = Entry({"title": "Hello", "summary": "x"})
    assert parse_entry(e) is None


def test_parse_entry_extracts_fields_and_date_from_published_parsed():
    published = time.gmtime(0)  # 1970-01-01 00:00:00 UTC
    e = Entry({
        "title": "Hello",
        "summary": "Résumé",
        "link": "https://example.com/x",
        "published_parsed": published,
    })
    out = parse_entry(e)
    assert out is not None
    assert out["title"] == "Hello"
    assert out["url"] == "https://example.com/x"
    assert isinstance(out["published_at"], dt.datetime)
