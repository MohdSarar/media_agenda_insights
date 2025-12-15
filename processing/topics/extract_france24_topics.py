from core.db import get_conn
from core.config import CONFIG
# processing/topics/extract_france24_topics.py

import os
import re
from core.logging import get_logger
from collections import defaultdict, Counter

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF

from typing import Any, Sequence
import datetime as dt
from core.db_types import PGConnection, PGCursor


load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
logger = get_logger(__name__)
# ----------------------------
# Stopwords + “bruit” par langue
# ----------------------------

STOP_FR = set(stopwords.words("french")) | {
    "france", "français", "française", "actualité", "actu", "info",
    # verbes/bruit fréquents
    "être", "avoir", "faire", "dire", "aller", "venir", "pouvoir", "vouloir", "devoir",
    "falloir", "savoir", "voir", "donner", "prendre", "mettre", "passer", "venir",
}

STOP_EN = set(stopwords.words("english")) | {
    "france", "news", "breaking", "latest",
    "be", "have", "do", "say", "go", "get", "make", "know", "think", "take", "see", "come", "want",
}

STOP_ES = set(stopwords.words("spanish")) | {
    "francia", "noticias", "última", "hora",
    "ser", "estar", "haber", "tener", "hacer", "decir", "poder", "ir", "ver", "dar", "saber", "querer",
}

STOP_AR = set(stopwords.words("arabic")) | {
    "فرنسا", "أخبار",
    # bruit très courant
    "قال", "وقالت", "يقول", "يقولون", "كان", "كانت", "يكون", "تم", "كما", "أي", "إن", "أن",
    "هذا", "هذه", "ذلك", "تلك", "الذي", "التي", "الذين",
    "في", "من", "إلى", "على", "عن", "مع", "بين", "بعد", "قبل",
}

LANG_STOPWORDS = {"fr": STOP_FR, "en": STOP_EN, "es": STOP_ES, "ar": STOP_AR}
DEFAULT_STOPWORDS = STOP_EN  # fallback




def normalize_lang(lang: str) -> str:
    if not lang:
        return "unknown"
    l = lang.lower().strip()
    if l.startswith("fr"):
        return "fr"
    if l.startswith("en"):
        return "en"
    if l.startswith("es"):
        return "es"
    if l.startswith("ar"):
        return "ar"
    return l


def preprocess_text(text: str, lang: str) -> str:
    """
    Nettoyage léger mais robuste multi-langues:
    - split unicode via \\w+
    - supprime digits, tokens courts, stopwords+bruit
    """
    if not text:
        return ""

    lang = normalize_lang(lang)
    sw = LANG_STOPWORDS.get(lang, DEFAULT_STOPWORDS)

    tokens = re.findall(r"\w+", text.lower(), flags=re.UNICODE)
    clean = []
    for t in tokens:
        if len(t) < 3:
            continue
        if any(ch.isdigit() for ch in t):
            continue
        if t in sw:
            continue
        clean.append(t)

    return " ".join(clean)


def fetch_docs_by_group(
    cur: PGCursor,
) -> list[tuple[dt.date, str, str, list[str]]]:
    """
    Retourne:
      (date, source, lang) -> [docs pré-nettoyés]
    Basé sur:
      articles_raw_f24 (source, lang, published_at...)
      articles_clean_f24 (cleaned_text, lang)
    """
    cur.execute("""
        SELECT
            ar.published_at::date AS date,
            ar.source,
            COALESCE(ac.lang, ar.lang) AS lang,
            ac.cleaned_text
        FROM articles_raw_f24 ar
        JOIN articles_clean_f24 ac ON ac.article_id = ar.id
        WHERE ar.published_at IS NOT NULL
        ORDER BY date, ar.id;
    """)
    rows = cur.fetchall()

    groups = defaultdict(list)
    for date, source, lang, cleaned_text in rows:
        lang = normalize_lang(lang)
        doc = preprocess_text(cleaned_text, lang)
        if not doc:
            continue
        groups[(date, source, lang)].append(doc)

    return groups


def already_computed_keys(cur: PGCursor) -> set[tuple[dt.date, str, str]]:
    """
    On évite de recalculer un (date, source, lang) déjà présent.
    """
    cur.execute("""
        SELECT DISTINCT date, source, lang
        FROM topics_daily_f24;
    """)
    return {(r[0], r[1], r[2]) for r in cur.fetchall()}


def extract_topics(
    docs: Sequence[str],
    n_topics: int | None = None,
    n_words: int = 8,
) -> list[dict[str, Any]]:
    """
    Topic modeling NMF sur TF-IDF.
    Retour:
      topic_keywords: dict(topic_id -> [keywords])
      topic_counts: Counter(topic_id -> nb_docs)
    """
    if n_topics is None:
        n_topics = int(CONFIG["topics"]["default_n_topics"])

    if not docs:
        return {}, Counter()

    # si trop peu de docs, NMF devient instable
    if len(docs) < int(CONFIG["topics"]["min_docs"]):
        return {}, Counter()

    n_components = min(n_topics, max(1, len(docs) // 2))

    vectorizer = TfidfVectorizer(
        max_features=5000,
        min_df=1,
        max_df=0.9,
        token_pattern=r"(?u)\b\w\w+\b",
    )
    tfidf = vectorizer.fit_transform(docs)
    if tfidf.shape[1] == 0:
        return {}, Counter()

    feature_names = vectorizer.get_feature_names_out()

    nmf = NMF(
        n_components=n_components,
        random_state=42,
        init="nndsvda",
        max_iter=300
    )

    W = nmf.fit_transform(tfidf)
    H = nmf.components_

    topic_keywords = {}
    for topic_id, weights in enumerate(H):
        top_idx = weights.argsort()[::-1][:n_words]
        keywords = [feature_names[i] for i in top_idx if feature_names[i]]
        topic_keywords[topic_id] = keywords

    doc_topics = W.argmax(axis=1)
    topic_counts = Counter(int(t) for t in doc_topics)

    return topic_keywords, topic_counts


def compute_france24_topics_daily() -> None:
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()

        try:
            groups = fetch_docs_by_group(cur)
            done = already_computed_keys(cur)

            logger.info(f"{len(groups)} groupes (date, source, lang) trouvés.")
            rows_to_insert = []

            # Pour un "ALL" par (date, lang) (utile pour une vue globale par langue)
            per_date_lang_docs = defaultdict(list)

            for (date, source, lang), docs in groups.items():
                if (date, source, lang) in done:
                    continue

                per_date_lang_docs[(date, lang)].extend(docs)

                topic_keywords, topic_counts = extract_topics(docs)
                if not topic_keywords:
                    logger.info(f"[{date}] {source}/{lang}: aucun topic détecté (docs={len(docs)}).")
                    continue

                for tid, keywords in topic_keywords.items():
                    count = int(topic_counts.get(tid, 0))
                    if count <= 0:
                        continue
                    topic_label = ", ".join(keywords[:3]) if keywords else f"topic_{tid}"

                    rows_to_insert.append((
                        date, source, lang, tid, topic_label, count, keywords
                    ))

            # ALL par (date, lang) — sans mixer les langues
            for (date, lang), docs in per_date_lang_docs.items():
                if (date, "ALL", lang) in done:
                    continue

                topic_keywords, topic_counts = extract_topics(docs)
                if not topic_keywords:
                    continue

                for tid, keywords in topic_keywords.items():
                    count = int(topic_counts.get(tid, 0))
                    if count <= 0:
                        continue
                    topic_label = ", ".join(keywords[:3]) if keywords else f"topic_{tid}"
                    rows_to_insert.append((
                        date, "ALL", lang, tid, topic_label, count, keywords
                    ))

            if not rows_to_insert:
                logger.info("Aucun topic France 24 à insérer.")
                conn.rollback()
                return

            logger.info(f"Insertion de {len(rows_to_insert)} lignes dans topics_daily_f24...")

            execute_values(
                cur,
                """
                INSERT INTO topics_daily_f24
                (date, source, lang, topic_id, topic_label, articles_count, keywords)
                VALUES %s
                ON CONFLICT (date, source, lang, topic_id)
                DO UPDATE SET
                topic_label = EXCLUDED.topic_label,
                articles_count = EXCLUDED.articles_count,
                keywords = EXCLUDED.keywords;

                """,
                rows_to_insert
            )

            conn.commit()
            logger.info("topics_daily_f24 mis à jour.")

        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur topics_daily_f24 : {e}")
            raise
        finally:
            cur.close()



if __name__ == "__main__":
    compute_france24_topics_daily()
