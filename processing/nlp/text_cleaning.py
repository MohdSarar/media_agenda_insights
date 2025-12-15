from __future__ import annotations

import re
from bs4 import BeautifulSoup


_URL_RE = re.compile(r"http\S+", re.IGNORECASE)
_IMG_RE = re.compile(r"\b(img|jpg|jpeg|png|gif)\b", re.IGNORECASE)
_SIZE_RE = re.compile(r"\b\d+x\d+\b")


def clean_html(text: str) -> str:
    """Nettoyage HTML + artefacts techniques (URLs, img/jpg/png, 800x0, entités)."""
    if not text:
        return ""

    soup = BeautifulSoup(text, "html.parser")
    clean = soup.get_text(separator=" ")

    clean = _URL_RE.sub(" ", clean)
    clean = _IMG_RE.sub(" ", clean)
    clean = _SIZE_RE.sub(" ", clean)

    clean = clean.replace("&nbsp;", " ").replace("&amp;", " ").replace("&quot;", " ")
    clean = clean.replace("><", " ")

    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def clean_text(text: str) -> str:
    """Normalisation simple (newlines -> spaces, strip)."""
    if not text:
        return ""
    return text.replace("\n", " ").strip()
