import os
import re
import json
import logging
from typing import Optional, Dict, Any, List, Tuple

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

# Optional libs (fallback if missing)
try:
    from langdetect import detect as ld_detect
except Exception:
    ld_detect = None

try:
    import spacy
except Exception:
    spacy = None

try:
    import stanza
except Exception:
    stanza = None

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_URL = os.getenv("DATABASE_URL")
BATCH_SIZE = int(os.getenv("SOCIAL_NLP_BATCH_SIZE", "200"))

# Regex utils
RE_URL = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
RE_MENTION = re.compile(r"@\w+")
RE_MULTI_SPACE = re.compile(r"\s+")
RE_HASHTAG = re.compile(r"#([A-Za-z0-9_À-ÿ\u0600-\u06FF]+)")

# A tiny stop/guard for absurdly small texts
MIN_CHARS_FOR_LANG = 30


def connect_db():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL introuvable. Vérifie ton .env")
    return psycopg2.connect(DB_URL)


def clean_text_basic(text: str) -> str:
    """
    Nettoyage léger orienté "signals".
    On ne fait PAS de heavy-normalization ici : on veut garder sens & noms propres.
    """
    if not text:
        return ""
    t = text

    # Remove URLs and mentions
    t = RE_URL.sub(" ", t)
    t = RE_MENTION.sub(" ", t)

    # Remove common markdown artifacts
    t = t.replace("\u200b", " ")  # zero-width space
    t = t.replace("`", " ")
    t = t.replace("*", " ")
    t = t.replace("_", " ")

    # Normalize spaces
    t = RE_MULTI_SPACE.sub(" ", t).strip()
    return t


def extract_hashtags(text: str) -> List[str]:
    if not text:
        return []
    tags = [m.group(1) for m in RE_HASHTAG.finditer(text)]
    # normalize: lowercase for latin scripts, keep arabic as-is
    norm = []
    for tag in tags:
        tag_clean = tag.strip()
        if not tag_clean:
            continue
        # simple heuristic: if mostly latin, lowercase
        if re.search(r"[A-Za-z]", tag_clean):
            tag_clean = tag_clean.lower()
        norm.append(tag_clean)
    # unique but stable order
    seen = set()
    out = []
    for t in norm:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out


def detect_lang(text: str) -> str:
    """
    Best-effort language detection.
    - If Arabic characters exist -> 'ar'
    - Else try langdetect if available
    - Else default 'fr'
    """
    if not text:
        return "unknown"

    # Arabic fast heuristic
    if re.search(r"[\u0600-\u06FF]", text):
        return "ar"

    if ld_detect and len(text) >= MIN_CHARS_FOR_LANG:
        try:
            lang = ld_detect(text)
            # map some codes if needed
            if lang in {"fr", "en", "es", "ar"}:
                return lang
            return lang  # keep whatever it returns
        except Exception:
            pass

    # Fallback
    return "fr"


def get_spacy_pipeline(lang: str):
    """
    Load spaCy model lazily.
    You can set env vars:
      SPACY_FR_MODEL=fr_core_news_md
      SPACY_EN_MODEL=en_core_web_md
      SPACY_ES_MODEL=es_core_news_md
    """
    if not spacy:
        return None

    model_env = {
        "fr": os.getenv("SPACY_FR_MODEL", "fr_core_news_md"),
        "en": os.getenv("SPACY_EN_MODEL", "en_core_web_md"),
        "es": os.getenv("SPACY_ES_MODEL", "es_core_news_md"),
    }.get(lang)

    if not model_env:
        return None

    try:
        return spacy.load(model_env, disable=["textcat"])
    except Exception as e:
        logging.warning("spaCy model not available for lang=%s (%s). Fallback simple tokenization.", lang, e)
        return None


_STANZA_PIPELINES: Dict[str, Any] = {}


def get_stanza_pipeline(lang: str):
    """
    Stanza is mainly useful for Arabic here.
    You can enable it if you already use Stanza in your project.
    """
    if not stanza:
        return None

    if lang != "ar":
        return None

    if lang in _STANZA_PIPELINES:
        return _STANZA_PIPELINES[lang]

    try:
        # Assumes stanza models are already downloaded in your environment.
        # If not, run once: stanza.download('ar')
        p = stanza.Pipeline(lang="ar", processors="tokenize,pos,lemma,ner", tokenize_no_ssplit=True, verbose=False)
        _STANZA_PIPELINES[lang] = p
        return p
    except Exception as e:
        logging.warning("Stanza pipeline not available for Arabic (%s). Fallback simple tokenization.", e)
        return None


def nlp_extract(lang: str, text: str) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
    """
    Returns: tokens, lemmas, entities
    """
    if not text:
        return [], [], []

    # Prefer Stanza for Arabic if available
    st = get_stanza_pipeline(lang)
    if st:
        doc = st(text)
        tokens = []
        lemmas = []
        for sent in doc.sentences:
            for w in sent.words:
                if w.text:
                    tokens.append(w.text)
                    lemmas.append(w.lemma if w.lemma else w.text)
        entities = []
        for ent in doc.ents:
            entities.append({"text": ent.text, "label": ent.type})
        return tokens, lemmas, entities

    # spaCy for fr/en/es if available
    nlp = get_spacy_pipeline(lang)
    if nlp:
        doc = nlp(text)
        tokens = [t.text for t in doc if not t.is_space]
        lemmas = [t.lemma_ if t.lemma_ else t.text for t in doc if not t.is_space]
        entities = [{"text": e.text, "label": e.label_} for e in doc.ents]
        return tokens, lemmas, entities

    # Fallback: naive tokenization
    # keep words and numbers, drop punctuation
    toks = re.findall(r"[\w\u0600-\u06FF]+", text, flags=re.UNICODE)
    return toks, toks, []


def fetch_unprocessed(conn, batch_size: int) -> List[Dict[str, Any]]:
    """
    Get rows from raw not yet in clean (idempotent).
    """
    sql = """
        SELECT r.platform, r.source, r.external_id, r.url, r.title, r.content, r.published_at
        FROM social_posts_raw r
        LEFT JOIN social_posts_clean c
          ON c.platform = r.platform AND c.external_id = r.external_id
        WHERE c.id IS NULL
        ORDER BY r.published_at DESC NULLS LAST, r.id DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (batch_size,))
        rows = cur.fetchall()

    out = []
    for row in rows:
        out.append({
            "platform": row[0],
            "source": row[1],
            "external_id": row[2],
            "url": row[3],
            "title": row[4],
            "content": row[5],
            "published_at": row[6],
        })
    return out


def upsert_clean(conn, rec: Dict[str, Any]):
    sql = """
        INSERT INTO social_posts_clean
          (platform, source, external_id, url, title, clean_text, lang, tokens, lemmas, entities, hashtags)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (platform, external_id) DO UPDATE SET
          source = EXCLUDED.source,
          url = EXCLUDED.url,
          title = EXCLUDED.title,
          clean_text = EXCLUDED.clean_text,
          lang = EXCLUDED.lang,
          tokens = EXCLUDED.tokens,
          lemmas = EXCLUDED.lemmas,
          entities = EXCLUDED.entities,
          hashtags = EXCLUDED.hashtags,
          processed_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                rec["platform"],
                rec["source"],
                rec["external_id"],
                rec.get("url"),
                rec.get("title"),
                rec.get("clean_text"),
                rec.get("lang"),
                Json(rec.get("tokens", [])),
                Json(rec.get("lemmas", [])),
                Json(rec.get("entities", [])),
                Json(rec.get("hashtags", [])),
            )
        )


def main():
    conn = connect_db()
    total = 0

    try:
        while True:
            batch = fetch_unprocessed(conn, BATCH_SIZE)
            if not batch:
                logging.info("No new social posts to process. Done.")
                break

            logging.info("Processing batch size=%s", len(batch))

            for row in batch:
                # IMPORTANT: if content is NULL, we use title (your observation is normal)
                base_text = row["content"] if row.get("content") else (row.get("title") or "")
                base_text = base_text.strip()

                hashtags = extract_hashtags(base_text)
                cleaned = clean_text_basic(base_text)

                lang = detect_lang(cleaned)
                tokens, lemmas, entities = nlp_extract(lang, cleaned)

                rec = {
                    "platform": row["platform"],
                    "source": row["source"],
                    "external_id": row["external_id"],
                    "url": row.get("url"),
                    "title": row.get("title"),
                    "clean_text": cleaned,
                    "lang": lang,
                    "tokens": tokens,
                    "lemmas": lemmas,
                    "entities": entities,
                    "hashtags": hashtags,
                }

                upsert_clean(conn, rec)
                total += 1

            conn.commit()
            logging.info("Committed batch. total_processed=%s", total)

    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Committing current transaction if possible.")
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
