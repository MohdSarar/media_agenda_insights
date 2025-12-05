import os
import logging

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

import stanza
import spacy

import re
from bs4 import BeautifulSoup


load_dotenv()

DB_URL = os.getenv("DATABASE_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

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


def get_db_connection():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL manquant dans l'environnement")
    return psycopg2.connect(DB_URL)


def fetch_unprocessed_articles(cur):
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


def clean_html(text: str) -> str:
    """
    Nettoyage HTML + artefacts techniques (img, 800x0, etc.)
    """
    if not text:
        return ""

    # Suppression des balises HTML
    soup = BeautifulSoup(text, "html.parser")
    clean = soup.get_text(separator=" ")

    # Suppression des URLs
    clean = re.sub(r'http\S+', ' ', clean)

    # Suppression des patterns d'images / tailles
    clean = re.sub(r'\b(img|jpg|jpeg|png|gif)\b', ' ', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b\d+x\d+\b', ' ', clean)  # ex: 800x0

    # Suppression de quelques entités HTML courantes
    clean = clean.replace("&nbsp;", " ")
    clean = clean.replace("&amp;", " ")
    clean = clean.replace("&quot;", " ")

    # Résidus de "><"
    clean = clean.replace("><", " ")

    # Espaces multiples
    clean = re.sub(r'\s+', ' ', clean)

    return clean.strip()


def clean_text(text: str) -> str:
    """
    Nettoyage général (retour à la ligne, espaces).
    Appelée après clean_html pour normaliser.
    """
    if not text:
        return ""
    return text.replace("\n", " ").strip()


def process_text_stanza_and_spacy(text: str):
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


def insert_clean(cur, article_id, cleaned, tokens, lemmas, ents):
    cur.execute("""
        INSERT INTO articles_clean (article_id, cleaned_text, tokens, lemmas, entities)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(article_id) DO NOTHING;
    """, (
        article_id,
        cleaned,
        tokens,
        lemmas,
        Json(ents)  # sérialisation JSONB correcte
    ))


def process_articles():
    conn = get_db_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        articles = fetch_unprocessed_articles(cur)
        logging.info(f"{len(articles)} articles à traiter.")

        count = 0

        for article_id, title, summary in articles:
            # Texte brut (titre + résumé)
            raw_text = f"{title or ''}. {summary or ''}"

            # 1) Nettoyage HTML
            html_cleaned = clean_html(raw_text)

            # 2) Nettoyage simple
            cleaned = clean_text(html_cleaned)

            # 3) NLP (Stanza + spaCy) sur le texte nettoyé
            tokens, lemmas, ents = process_text_stanza_and_spacy(cleaned)

            # 4) Insertion en base
            insert_clean(cur, article_id, cleaned, tokens, lemmas, ents)
            count += 1

        conn.commit()
        logging.info(f"Traitement NLP (Stanza + spaCy) terminé. Articles nettoyés : {count}")

    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur NLP (Stanza + spaCy) : {e}")
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    process_articles()
