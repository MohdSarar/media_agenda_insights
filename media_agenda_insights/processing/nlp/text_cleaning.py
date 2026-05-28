from __future__ import annotations

import re
from bs4 import BeautifulSoup


_URL_RE = re.compile(r"http\S+", re.IGNORECASE)
_IMG_RE = re.compile(r"\b(img|jpg|jpeg|png|gif)\b", re.IGNORECASE)
_SIZE_RE = re.compile(r"\b\d+x\d+\b")

# Typographic/curly apostrophes and look-alikes → ASCII apostrophe
_TYPO_APOS_RE = re.compile(r"[‘’ʼʻˈ`´]")

# French elision prefixes that Stanza may fail to split when the apostrophe is
# typographic. We expand them so the tokeniser sees two proper words.
_ELISION_RE = re.compile(
    r"\b(j|l|d|s|c|m|t|n|qu|jusqu|lorsqu|puisqu|quoiqu)'",
    re.IGNORECASE,
)
_ELISION_MAP = {
    "j": "je ", "l": "le ", "d": "de ", "s": "se ", "c": "ce ",
    "m": "me ", "t": "te ", "n": "ne ", "qu": "que ", "jusqu": "jusqu ",
    "lorsqu": "lorsque ", "puisqu": "puisque ", "quoiqu": "quoique ",
}


def normalize_apostrophes(text: str) -> str:
    """Replace all typographic apostrophe variants with ASCII ' (U+0027)."""
    return _TYPO_APOS_RE.sub("'", text)


def expand_elisions(text: str) -> str:
    """
    Expand French contractions so Stanza tokenises them as two words.
    Example: "j'ai" → "je ai", "l'état" → "le état".
    Must be called after normalize_apostrophes().
    """
    def _replace(m: re.Match) -> str:
        prefix = m.group(1).lower()
        return _ELISION_MAP.get(prefix, prefix + " ")

    return _ELISION_RE.sub(_replace, text)


def clean_html(text: str) -> str:
    """Nettoyage HTML + artefacts techniques (URLs, img/jpg/png, 800x0, entités)."""
    if not text:
        return ""

    soup = BeautifulSoup(text, "html.parser")
    clean = soup.get_text(separator=" ")

    # Normalise apostrophes BEFORE any other processing
    clean = normalize_apostrophes(clean)

    clean = _URL_RE.sub(" ", clean)
    clean = _IMG_RE.sub(" ", clean)
    clean = _SIZE_RE.sub(" ", clean)

    clean = clean.replace("&nbsp;", " ").replace("&amp;", " ").replace("&quot;", " ")
    clean = clean.replace("><", " ")

    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def clean_text(text: str) -> str:
    """Normalisation (newlines → spaces, apostrophe normalisation, elision expansion)."""
    if not text:
        return ""
    text = text.replace("\n", " ")
    text = normalize_apostrophes(text)
    text = expand_elisions(text)
    return text.strip()
