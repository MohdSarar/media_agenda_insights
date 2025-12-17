from __future__ import annotations

from processing.nlp.text_cleaning import clean_html, clean_text


def test_clean_html_removes_tags_urls_images_sizes_and_entities():
    html = """<p>Bonjour <b>monde</b> &nbsp; <img src='x.jpg'/> 800x0
    https://example.com/test &amp; fin</p>"""
    out = clean_html(html)
    assert "<" not in out and ">" not in out
    assert "http" not in out
    assert "jpg" not in out.lower()
    assert "800x0" not in out
    assert "&nbsp" not in out
    assert "Bonjour" in out


def test_clean_text_removes_newlines_and_strips():
    out = clean_text("  a\n b  ")
    assert out == "a  b"
