import os
import logging
from collections import Counter, defaultdict

import psycopg2
from dotenv import load_dotenv
import spacy
from psycopg2.extras import execute_values

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

nlp = spacy.load("fr_core_news_sm")
STOPWORDS = set(nlp.Defaults.stop_words)


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


def fetch_lemmas_by_day(cur):
    """
    Retourne :
      (date, source, media_type) -> [liste de listes de lemmes]
    """
    cur.execute("""
        SELECT
            ar.published_at::date AS date,
            ar.source,
            ar.media_type,
            ac.lemmas
        FROM articles_raw ar
        JOIN articles_clean ac ON ac.article_id = ar.id
        WHERE ar.published_at IS NOT NULL
        ORDER BY date, source;
    """)
    rows = cur.fetchall()

    groups = defaultdict(list)
    for date, source, media_type, lemmas in rows:
        if lemmas:
            groups[(date, source, media_type)].append(lemmas)

    return groups


def already_computed_dates(cur):
    cur.execute("SELECT DISTINCT date FROM keywords_daily;")
    return {row[0] for row in cur.fetchall()}


def build_word_counts(lemmas_lists):
    counter = Counter()
    for lemmas in lemmas_lists:
        for lemma in lemmas:
            if not lemma:
                continue
            w = lemma.lower().strip()

            if len(w) < 3:
                continue
            if any(ch.isdigit() for ch in w):
                continue
            if w in USELESS_WORDS:
                continue

            counter[w] += 1
    return counter


def compute_keywords_daily():
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        groups = fetch_lemmas_by_day(cur)
        done_dates = already_computed_dates(cur)

        logging.info(f"{len(groups)} groupes (date, source, media_type) trouvés.")
        rows_to_insert = []

        per_date_media = defaultdict(Counter)  # (date, media_type) -> Counter
        per_date_all = defaultdict(Counter)    # date -> Counter global

        # 1) par (date, source, media_type)
        for (date, source, media_type), lemmas_lists in groups.items():
            if date in done_dates:
                continue

            counter = build_word_counts(lemmas_lists)
            if not counter:
                continue

            per_date_media[(date, media_type)] += counter
            per_date_all[date] += counter

            top10 = counter.most_common(10)
            for rank, (word, count) in enumerate(top10, start=1):
                rows_to_insert.append(
                    (date, source, media_type, word, count, rank)
                )

        # 2) 'ALL' pour chaque (date, media_type)
        for (date, media_type), counter in per_date_media.items():
            if date in done_dates:
                continue
            top10 = counter.most_common(10)
            for rank, (word, count) in enumerate(top10, start=1):
                rows_to_insert.append(
                    (date, "ALL", media_type, word, count, rank)
                )

        # 3) 'ALL' global (date, ALL, ALL)
        for date, counter in per_date_all.items():
            if date in done_dates:
                continue
            top10 = counter.most_common(10)
            for rank, (word, count) in enumerate(top10, start=1):
                rows_to_insert.append(
                    (date, "ALL", "ALL", word, count, rank)
                )

        if not rows_to_insert:
            logging.info("Aucun nouveau mot-clé à insérer.")
            conn.rollback()
            return

        logging.info(f"Insertion de {len(rows_to_insert)} lignes dans keywords_daily...")

        execute_values(
            cur,
            """
            INSERT INTO keywords_daily (date, source, media_type, word, count, rank)
            VALUES %s
            ON CONFLICT (date, source, media_type, word)
            DO UPDATE SET
              count = EXCLUDED.count,
              rank = EXCLUDED.rank
            """,
            rows_to_insert
        )

        conn.commit()
        logging.info("keywords_daily mis à jour.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur extraction keywords : {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    compute_keywords_daily()
