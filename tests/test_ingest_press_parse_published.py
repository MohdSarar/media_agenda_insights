from __future__ import annotations

import time
import datetime as dt
from types import SimpleNamespace

from ingestion.presse.ingest_press import parse_published


def test_parse_published_uses_published_parsed_when_available():
    e = SimpleNamespace(published_parsed=time.gmtime(0), updated_parsed=None)
    out = parse_published(e)
    assert isinstance(out, dt.datetime)
    # Should be epoch start in local/naive time; we only assert it's not None
    assert out.year == 1970


def test_parse_published_returns_none_when_no_date_fields():
    e = SimpleNamespace()
    out = parse_published(e)
    assert out is None
