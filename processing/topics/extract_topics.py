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


def get_conn():
    return psycopg2.connect(DB_URL)


def clean_lemmas(lemmas):
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


def fetch_tv_docs_by_day(cur):
    """
    Retourne date -> liste de (article_id, texte_doc)
    où texte_doc = lemmes nettoyés joinés par espace.
    """
    cur.execute("""
        SELECT
            ar.id,
            ar.published_at::date AS date,
            ac.lemmas
        FROM articles_raw ar
        JOIN articles_clean ac ON ac.article_id = ar.id
        WHERE ar.published_at IS NOT NULL
          AND ar.media_type = 'tv'
        ORDER BY date, ar.id;
    """)
    rows = cur.fetchall()

    docs_by_date = defaultdict(list)
    for article_id, date, lemmas in rows:
        if not lemmas:
            continue
        cleaned = clean_lemmas(lemmas)
        if not cleaned:
            continue
        text = " ".join(cleaned)
        docs_by_date[date].append((article_id, text))

    return docs_by_date


def already_computed_dates(cur):
    cur.execute("""
        SELECT DISTINCT date
        FROM topics_daily
        WHERE media_type = 'tv';
    """)
    return {row[0] for row in cur.fetchall()}


def extract_topics_for_date(date, docs, n_topics=10, n_words=8):
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


def compute_topics_daily():
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

            article_ids = [a_id for (a_id, txt) in articles]
            docs = [txt for (a_id, txt) in articles]

            if len(docs) < 3:
                logging.info(f"[{date}] Trop peu de docs ({len(docs)}), on ignore.")
                continue

            logging.info(f"[{date}] {len(docs)} docs -> topic modeling...")

            topics_info, doc_topic_ids = extract_topics_for_date(date, docs)

            if not topics_info:
                logging.info(f"[{date}] aucun topic détecté.")
                continue

            topic_counts = Counter(doc_topic_ids)

            for t in topics_info:
                tid = int(t["topic_id"])
                keywords = t["keywords"]
                articles_count = int(topic_counts.get(tid, 0))
                topic_label = ", ".join(keywords[:3])

                rows_to_insert.append((
                    date,
                    "ALL",          # toutes chaînes TV
                    "tv",
                    tid,
                    topic_label,
                    articles_count,
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
