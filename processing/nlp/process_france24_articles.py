# processing/nlp/process_france24_articles.py

import os
import logging
import re

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

import spacy
from langdetect import detect, LangDetectException

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FR24-NLP] %(message)s"
)

# spaCy FR (fallback)
NLP_FR = spacy.load("fr_core_news_sm")

# -----------------------------
# Utils
# -----------------------------

def get_conn():
    return psycopg2.connect(DB_URL)


def clean_text(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"<[^>]+>", " ", text)      # HTML
    text = re.sub(r"http\S+", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def detect_language(text: str, source: str) -> str:
    """
    Priorité :
    1) source france24_xx
    2) détection automatique fallback
    """

    if source.endswith("_fr"):
        return "fr"
    if source.endswith("_en"):
        return "en"
    if source.endswith("_es"):
        return "es"
    if source.endswith("_ar"):
        return "ar"

    try:
        return detect(text)
    except LangDetectException:
        return "fr"


def nlp_process(text: str, lang: str):
    """
    NLP minimal propre (token / lemma)
    Pour FR : spaCy
    Pour autres langues : tokenisation simple
    """

    if lang == "fr":
        doc = NLP_FR(text)
        tokens = [t.text.lower() for t in doc if t.is_alpha]
        lemmas = [t.lemma_.lower() for t in doc if t.is_alpha]
        return tokens, lemmas

    # fallback simple (EN / ES / AR)
    tokens = [
        w.lower()
        for w in re.findall(r"\b\w+\b", text)
        if len(w) > 2
    ]
    return tokens, tokens


# -----------------------------
# Main
# -----------------------------

def process_france24_articles():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                ar.id,
                ar.source,
                COALESCE(ar.summary, '') || ' ' || ar.title AS text
            FROM articles_raw_f24 ar
            WHERE NOT EXISTS (
                SELECT 1
                FROM articles_clean_f24 ac
                WHERE ac.article_id = ar.id
            )
        """)

        rows = cur.fetchall()
        logging.info(f"{len(rows)} articles France 24 à traiter.")

        if not rows:
            return

        to_insert = []

        for article_id, source, raw_text in rows:
            cleaned = clean_text(raw_text)
            if not cleaned:
                continue

            lang = detect_language(cleaned, source)
            tokens, lemmas = nlp_process(cleaned, lang)

            to_insert.append((
                article_id,
                cleaned,
                tokens,
                lemmas,
                lang
            ))

        if not to_insert:
            logging.info("Aucun article NLP exploitable.")
            return

        execute_values(
            cur,
            """
            INSERT INTO articles_clean_f24
                (article_id, cleaned_text, tokens, lemmas, lang)
            VALUES %s
            ON CONFLICT (article_id) DO NOTHING
            """,
            to_insert
        )

        conn.commit()
        logging.info(f"{len(to_insert)} articles France 24 traités (NLP).")

    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur NLP France 24 : {e}")
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    process_france24_articles()
