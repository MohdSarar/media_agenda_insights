from core.db import get_conn
import os
from core.logging import get_logger
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

import stanza
import spacy

import re
from bs4 import BeautifulSoup

from processing.nlp.text_cleaning import clean_html, clean_text

from typing import Any, Optional
from core.db_types import PGConnection, PGCursor, JsonDict, JsonList

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
logger = get_logger(__name__)
# ⚠️ À exécuter UNE SEULE FOIS dans un script à part ou en shell :
# import stanza; stanza.download('fr')
# python -m spacy download fr_core_news_sm

# Pipeline Stanza pour tokenisation + lemmatisation
stanza_nlp = stanza.Pipeline(
    lang="fr",
    processors="tokenize,lemma,pos",
    use_gpu=False
)

# spaCy pour les entités (NER) uniquement
spacy_nlp = spacy.load("fr_core_news_sm")


get_db_connection = get_conn

def fetch_unprocessed_articles(
        cur: PGCursor,
    ) -> list[tuple[int, str, Optional[str]]]:
    """
    Récupère tous les articles qui n'ont pas encore de ligne associée
    dans articles_clean.
    """
    cur.execute("""
        SELECT id, title, summary
        FROM articles_raw
        WHERE id NOT IN (SELECT article_id FROM articles_clean)
        ORDER BY id ASC;
    """)
    return cur.fetchall()




def process_text_stanza_and_spacy(
        text: str,
    ) -> tuple[list[str], list[str], list[JsonDict]]:
        
    """
    Utilise Stanza pour tokens + lemmes
    et spaCy pour les entités nommées.
    """
    # --- Stanza : tokens + lemmes ---
    doc_stz = stanza_nlp(text)
    tokens = []
    lemmas = []

    for sent in doc_stz.sentences:
        for word in sent.words:
            tokens.append(word.text)
            lemmas.append(word.lemma)

    # --- spaCy : entités nommées ---
    doc_sp = spacy_nlp(text)
    ents = [
        {"text": ent.text, "label": ent.label_}
        for ent in doc_sp.ents
    ]

    return tokens, lemmas, ents


def insert_clean(
    cur: PGCursor,
    article_id: int,
    cleaned: str,
    tokens: list[str],
    lemmas: list[str],
    ents: list[JsonDict],
) -> None:
    cur.execute("""
        INSERT INTO articles_clean (article_id, cleaned_text, tokens, lemmas, entities)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(article_id) DO NOTHING;
    """, (
        article_id,
        cleaned,
        tokens,
        lemmas,
        Json(ents)  #sérialisation JSONB correcte
    ))


def process_articles()  -> None:
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()

        try:
            articles = fetch_unprocessed_articles(cur)
            logger.info(f"{len(articles)} articles à traiter.")

            count = 0

            for article_id, title, summary in articles:
                #texte brut (titre + résumé)
                raw_text = f"{title or ''}. {summary or ''}"

                # nettoyage HTML
                html_cleaned = clean_html(raw_text)

                #nettoyage simple
                cleaned = clean_text(html_cleaned)

                # NLP (Stanza + spaCy) sur le texte nettoyé
                tokens, lemmas, ents = process_text_stanza_and_spacy(cleaned)

                insert_clean(cur, article_id, cleaned, tokens, lemmas, ents)
                count += 1

            conn.commit()
            logger.info(f"Traitement NLP (Stanza + spaCy) terminé. Articles nettoyés : {count}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur NLP (Stanza + spaCy) : {e}")
            raise

        finally:
            cur.close()
        


if __name__ == "__main__":
    process_articles()
