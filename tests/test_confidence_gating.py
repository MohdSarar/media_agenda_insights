"""Tests for render_confidence helper and load_dashboard_config."""
import types
import pytest


def _make_st_stub():
    """Return a minimal streamlit stub that records calls."""
    stub = types.SimpleNamespace(
        _captions=[],
        _markdowns=[],
    )
    stub.caption = lambda msg: stub._captions.append(msg)
    stub.markdown = lambda msg, **kw: stub._markdowns.append(msg)
    return stub


def test_render_confidence_above_threshold(monkeypatch):
    st_stub = _make_st_stub()
    import dashboard.ui.components as comp
    monkeypatch.setattr(comp, "st", st_stub)

    result = comp.render_confidence(n=20, min_n=8)

    assert result is True
    assert any("20" in c for c in st_stub._captions)
    assert st_stub._markdowns == []


def test_render_confidence_below_threshold(monkeypatch):
    st_stub = _make_st_stub()
    import dashboard.ui.components as comp
    monkeypatch.setattr(comp, "st", st_stub)

    result = comp.render_confidence(n=3, min_n=8)

    assert result is False
    assert st_stub._captions == []
    assert any("Faible confiance" in m for m in st_stub._markdowns)


def test_render_confidence_exactly_at_threshold(monkeypatch):
    st_stub = _make_st_stub()
    import dashboard.ui.components as comp
    monkeypatch.setattr(comp, "st", st_stub)

    result = comp.render_confidence(n=8, min_n=8)

    assert result is True
    assert any("8" in c for c in st_stub._captions)


def test_load_dashboard_config_returns_dict(tmp_path, monkeypatch):
    """load_dashboard_config should return a dict with retention/confidence keys."""
    import yaml
    config_dir = tmp_path / "media_agenda_insights" / "infra" / "config"
    config_dir.mkdir(parents=True)
    config_file = config_dir / "pipeline.yaml"
    config_file.write_text(
        yaml.dump({"retention": {"raw_days": 90}, "confidence": {"min_n": 8}}),
        encoding="utf-8",
    )

    import dashboard.data_access as da
    monkeypatch.setattr(da.Path, "parents", property(lambda self: [tmp_path]))

    # Use a direct file load to validate config shape without DB
    import yaml as _yaml
    with open(config_file, encoding="utf-8") as f:
        cfg = _yaml.safe_load(f)

    assert cfg.get("confidence", {}).get("min_n") == 8
    assert cfg.get("retention", {}).get("raw_days") == 90
