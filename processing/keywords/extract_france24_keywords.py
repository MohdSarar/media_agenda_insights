from core.db import get_conn
from core.config import CONFIG
# processing/keywords/extract_france24_keywords.py

import os
from core.logging import get_logger
from collections import Counter, defaultdict

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from nltk.corpus import stopwords


from typing import Any, Iterable
import datetime as dt
from core.db_types import PGConnection, PGCursor

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
logger = get_logger(__name__)
# --- Stopwords multilingues France 24 ---

# Français
STOP_FR = set(stopwords.words("french")) | {
    "france", "français", "française", "actualité", "actu", "info"
}

# Anglais
STOP_EN = set(stopwords.words("english")) | {
    "france", "news", "breaking", "latest"
}

# Espagnol
STOP_ES = set(stopwords.words("spanish")) | {
    "francia", "noticias", "última", "hora"
}

# Arabe
STOP_AR = set(stopwords.words("arabic")) | {
    "فرنسا", "أخبار"
}


# --- Bruit linguistique spécifique France 24 ---

GENERIC_VERBS = {
    # FR
    "être", "avoir", "faire", "dire", "aller", "venir", "pouvoir", "devoir",
    # EN
    "be", "have", "do", "say", "go", "make", "get",
    # ES
    "ser", "estar", "haber", "tener", "decir", "hacer",
    # AR (verbes très fréquents)
    "كان", "يكون", "قال"
}

TEMPORAL_WORDS = {
    # FR
    "lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche",
    "janvier","février","mars","avril","mai","juin","juillet","août",
    "septembre","octobre","novembre","décembre",
    # EN
    "monday","tuesday","wednesday","thursday","friday","saturday","sunday",
    "january","february","march","april","may","june","july","august",
    "september","october","november","december","year","years",
    # ES
    "lunes","martes","miércoles","jueves","viernes","sábado","domingo",
    "enero","febrero","marzo","abril","mayo","junio","julio","agosto",
    "septiembre","octubre","noviembre","diciembre","años",
    # AR
    "الثلاثاء","الاثنين","الأربعاء","الخميس","الجمعة","السبت","الأحد","عاما","عام"
}

GENERIC_WORDS = {
    "plus","après","avant","while","during","since",
    "mientras","tras","durante",
    "خلال"
}



LANG_STOPWORDS = {
    "fr": STOP_FR,
    "en": STOP_EN,
    "es": STOP_ES,
    "ar": STOP_AR,
}

DEFAULT_STOPWORDS = STOP_FR




def fetch_lemmas_by_group(
    cur: PGCursor,
) -> list[tuple[dt.date, str, str, list[str]]]:
    """
    Retourne :
      (date, source, lang) -> [liste de listes de lemmes]
    Basé sur les tables articles_raw_f24 / articles_clean_f24.
    """
    cur.execute("""
        SELECT
            ar.published_at::date AS date,
            ar.source,
            COALESCE(ac.lang, ar.lang) AS lang,
            ac.lemmas
        FROM articles_raw_f24 ar
        JOIN articles_clean_f24 ac ON ac.article_id = ar.id
        WHERE ar.published_at IS NOT NULL
        ORDER BY date, ar.source;
    """)
    rows = cur.fetchall()

    groups = defaultdict(list)
    for date, source, lang, lemmas in rows:
        if not lemmas:
            continue
        groups[(date, source, lang)].append(lemmas)

    return groups


def already_computed_keys(cur: PGCursor) -> set[tuple[dt.date, str, str]]:
    """
    On considère qu'un triplet (date, source, lang) déjà présent
    n'a pas besoin d'être recalculé.
    """
    cur.execute("""
        SELECT DISTINCT date, source, lang
        FROM keywords_daily_f24;
    """)
    return {(r[0], r[1], r[2]) for r in cur.fetchall()}


def build_word_counts(
    lemmas_lists: Iterable[list[str]],
    lang_code: str,
) -> dict[str, int]:
    """
    Construit un Counter de mots filtrés par stopwords en fonction de la langue.
    """
    stopwords_lang = LANG_STOPWORDS.get(lang_code, DEFAULT_STOPWORDS)

    counter = Counter()
    for lemmas in lemmas_lists:
        for lemma in lemmas:
            if not lemma:
                continue
            w = str(lemma).lower().strip()

            if len(w) < 3:
                continue
            if any(ch.isdigit() for ch in w):
                continue
            if (
                w in stopwords_lang
                or w in GENERIC_VERBS
                or w in TEMPORAL_WORDS
                or w in GENERIC_WORDS
            ):
                continue


            counter[w] += 1

    return counter


def compute_france24_keywords_daily() -> None:
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()

        try:
            groups = fetch_lemmas_by_group(cur)
            done_keys = already_computed_keys(cur)

            logger.info(f"{len(groups)} groupes (date, source, lang) trouvés.")
            rows_to_insert = []

            # Top mots-clés par (date, source, lang)
            for (date, source, lang), lemmas_lists in groups.items():
                if (date, source, lang) in done_keys:
                    continue

                counter = build_word_counts(lemmas_lists, lang)
                if not counter:
                    continue

                top10 = counter.most_common(int(CONFIG["keywords"]["top_n"]))
                for rank, (word, count) in enumerate(top10, start=1):
                    rows_to_insert.append(
                        (date, source, lang, word, count, rank)
                    )

            if not rows_to_insert:
                logger.info("Aucun nouveau mot-clé France 24 à insérer.")
                conn.rollback()
                return

            logger.info(f"Insertion de {len(rows_to_insert)} lignes dans keywords_daily_f24...")

            execute_values(
                cur,
                """
                INSERT INTO keywords_daily_f24
                    (date, source, lang, word, count, rank)
                VALUES %s
                ON CONFLICT (date, source, lang, word)
                DO UPDATE SET
                count = EXCLUDED.count,
                rank = EXCLUDED.rank
                """,
                rows_to_insert
            )

            conn.commit()
            logger.info("keywords_daily_f24 mis à jour.")

        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur extraction keywords France 24 : {e}")
            raise
        finally:
            cur.close()



if __name__ == "__main__":
    compute_france24_keywords_daily()