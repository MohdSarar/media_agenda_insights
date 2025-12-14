import os
import logging
from collections import defaultdict, Counter

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


import spacy
from spacy.lang.fr.stop_words import STOP_WORDS as SPACY_STOP
from nltk.corpus import stopwords

from typing import Any, Iterable, Sequence
import datetime as dt
from core.db_types import PGConnection, PGCursor


NLTK_STOP = set(stopwords.words("french"))

CUSTOM_STOPWORDS = {
    'fin','par','pas','plus','moins','toujours','jamais','déjà',
    'faire','fait','être','avoir','mettre','donner','falloir','pouvoir','vouloir',
    'chez','entre','dont','sauf','selon','ainsi','cependant','tandis','lorsque','lorsqu',
    'afin','car','comme','contre',
    'cela','ça','ce','ces','cet','cette',
    'qui','que','quoi', '<br','><br','<br>','br','img','jpg','jpeg','src','src=','http','https','800x0','alt','width','height',
    'image','figure','amp','nbsp','quot','..','...','.',',',';',':','!','?','-','—','(',')','"',"'",'`',
    'lundi','mardi','mercredi','jeudi','vendredi','samedi','dimanche',
    'janvier','février','mars','avril','mai','juin','juillet','août','septembre','octobre','novembre','décembre',
    'dernier','dernière','année','mois','jour','semaine','actualité','actu','info',
    'être','avoir','faire','pouvoir','devoir','aller','venir',
    'on','lui','moi','toi','elle','ils','elles','nous','vous','ce','cette','ces','son','ses','leur','leurs',
    'tout','tous','plus','moins','sans','avec',
    'france','français','française'
}

USELESS_WORDS = SPACY_STOP | NLTK_STOP | CUSTOM_STOPWORDS


def get_conn() -> PGConnection:
    return psycopg2.connect(DB_URL)


def clean_lemmas(lemmas: Sequence[str]) -> list[str]:
    cleaned = []
    for l in lemmas:
        if not l:
            continue
        w = l.lower().strip()
        if len(w) < 3:
            continue
        if any(ch.isdigit() for ch in w):
            continue
        if w in USELESS_WORDS:
            continue
        cleaned.append(w)
    return cleaned


def fetch_tv_docs_by_day(
    cur: PGCursor,
) -> dict[dt.date, list[tuple[int, str]]]:
    """
    Retourne date -> liste de (article_id, source, texte_doc)
    où texte_doc = lemmes nettoyés joinés par espace.
    """
    cur.execute("""
        SELECT
            ar.id,
            ar.published_at::date AS date,
            ar.source,
            ac.lemmas
        FROM articles_raw ar
        JOIN articles_clean ac ON ac.article_id = ar.id
        WHERE ar.published_at IS NOT NULL
          AND ar.media_type = 'tv'
        ORDER BY date, ar.id;
    """)
    rows = cur.fetchall()

    docs_by_date = defaultdict(list)
    for article_id, date, source, lemmas in rows:
        if not lemmas:
            continue
        cleaned = clean_lemmas(lemmas)
        if not cleaned:
            continue
        text = " ".join(cleaned)
        # on garde maintenant aussi la source
        docs_by_date[date].append((article_id, source, text))

    return docs_by_date



def already_computed_dates(cur: PGCursor) -> set[dt.date]:
    cur.execute("""
        SELECT DISTINCT date
        FROM topics_daily
        WHERE media_type = 'tv';
    """)
    return {row[0] for row in cur.fetchall()}


def extract_topics_for_date(
    date: dt.date,
    docs: Sequence[tuple[int, str]],
    n_topics: int = 10,
    n_words: int = 8,
) -> list[dict[str, Any]]:
    """
    docs : liste de textes (1 par article)
    Retourne :
      - topics_info : [{topic_id, keywords}]
      - doc_topic_ids : liste du topic principal pour chaque doc
    """
    if len(docs) == 0:
        return [], []

    n_components = min(n_topics, max(1, len(docs)//2))
    if n_components <= 0:
        n_components = 1

    vectorizer = TfidfVectorizer(
        max_features=5000,
        min_df=1,
        max_df=0.9
    )
    tfidf = vectorizer.fit_transform(docs)
    feature_names = vectorizer.get_feature_names_out()

    nmf = NMF(
        n_components=n_components,
        random_state=42,
        init="nndsvda",
        max_iter=300
    )
    W = nmf.fit_transform(tfidf)
    H = nmf.components_

    topics_info = []
    for topic_idx in range(n_components):
        topic_weights = H[topic_idx]
        top_indices = topic_weights.argsort()[::-1][:n_words]
        keywords = [feature_names[i] for i in top_indices]
        topics_info.append({
            "topic_id": topic_idx,
            "keywords": keywords
        })

    doc_topic_ids = W.argmax(axis=1)

    return topics_info, doc_topic_ids


def compute_topics_daily() -> None:
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        docs_by_date = fetch_tv_docs_by_day(cur)
        done_dates = already_computed_dates(cur)

        logging.info(f"{len(docs_by_date)} dates avec docs TV.")

        rows_to_insert = []

        for date, articles in docs_by_date.items():
            if date in done_dates:
                logging.info(f"[{date}] déjà traitée, on saute.")
                continue

            # articles = liste de (article_id, source, texte)
            article_ids = [a_id for (a_id, src, txt) in articles]
            sources = [src for (a_id, src, txt) in articles]
            docs = [txt for (a_id, src, txt) in articles]

            if len(docs) < 3:
                logging.info(f"[{date}] Trop peu de docs ({len(docs)}), on ignore.")
                continue

            logging.info(f"[{date}] {len(docs)} docs -> topic modeling...")

            topics_info, doc_topic_ids = extract_topics_for_date(date, docs)

            if not topics_info:
                logging.info(f"[{date}] aucun topic détecté.")
                continue

            # comptage global par topic
            topic_counts = Counter(doc_topic_ids)

            # comptage par (source, topic_id)
            source_topic_counts = Counter()
            for src, topic_id in zip(sources, doc_topic_ids):
                source_topic_counts[(src, int(topic_id))] += 1

            # on garde la liste des sources présentes ce jour-là
            unique_sources = sorted(set(sources))

            for t in topics_info:
                tid = int(t["topic_id"])
                keywords = t["keywords"]
                topic_label = ", ".join(keywords[:3])

                # 1) lignes par chaîne TV
                for src in unique_sources:
                    src_count = int(source_topic_counts.get((src, tid), 0))
                    if src_count == 0:
                        continue

                    rows_to_insert.append((
                        date,
                        src,        # vraie source : cnews, bfmtv, etc.
                        "tv",
                        tid,
                        topic_label,
                        src_count,
                        keywords
                    ))

                # 2) ligne agrégée "ALL" (pour dashboard current)
                total_count = int(topic_counts.get(tid, 0))
                if total_count > 0:
                    rows_to_insert.append((
                        date,
                        "ALL",
                        "tv",
                        tid,
                        topic_label,
                        total_count,
                        keywords
                    ))

        if not rows_to_insert:
            logging.info("Aucun topic à insérer.")
            conn.rollback()
            return

        logging.info(f"Insertion de {len(rows_to_insert)} lignes dans topics_daily...")

        execute_values(
            cur,
            """
            INSERT INTO topics_daily
            (date, source, media_type, topic_id, topic_label, articles_count, keywords)
            VALUES %s
            ON CONFLICT (date, source, media_type, topic_id)
            DO UPDATE SET
              topic_label = EXCLUDED.topic_label,
              articles_count = EXCLUDED.articles_count,
              keywords = EXCLUDED.keywords
            """,
            rows_to_insert
        )

        conn.commit()
        logging.info("topics_daily mis à jour.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur topics_daily : {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    compute_topics_daily()
