from __future__ import annotations

import datetime as dt
import pytest

from core.schemas import RSSArticle


def _base_payload() -> dict:
    return {
        "source": "TEST_SOURCE",
        "category": "top_stories",
        "title": "Un titre valide",
        "content": "<p>contenu</p>",
        "url": "https://example.com/a",
        "published_at": dt.datetime(2025, 12, 1, 10, 0, 0),
        "lang": "fr",
    }


def test_rssarticle_trims_title_and_enforces_minlen():
    payload = _base_payload()
    payload["title"] = "   Bonjour monde   "
    a = RSSArticle.model_validate(payload)
    assert a.title == "Bonjour monde"


def test_rssarticle_normalizes_lang_variants():
    payload = _base_payload()
    payload["lang"] = "fr-fr"
    a = RSSArticle.model_validate(payload)
    assert a.lang == "fr"


def test_rssarticle_makes_datetime_tzaware_utc_when_naive():
    payload = _base_payload()
    payload["published_at"] = dt.datetime(2025, 12, 1, 10, 0, 0)  # naive
    a = RSSArticle.model_validate(payload)
    assert a.published_at.tzinfo is not None
    # validator sets UTC when tzinfo is missing
    assert a.published_at.tzinfo == dt.timezone.utc


def test_rssarticle_rejects_blank_category():
    payload = _base_payload()
    payload["category"] = "   "
    with pytest.raises(Exception):
        RSSArticle.model_validate(payload)
