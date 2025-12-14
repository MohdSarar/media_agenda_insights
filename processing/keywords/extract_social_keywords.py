import os
import re
import logging
import datetime as dt
from collections import defaultdict

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from sklearn.feature_extraction.text import TfidfVectorizer

# NLTK stopwords
import nltk
from nltk.corpus import stopwords as nltk_stopwords
from typing import Any, Iterable, Sequence
from core.db_types import PGConnection

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_URL = os.getenv("DATABASE_URL")

DAYS_BACK = int(os.getenv("SOCIAL_KEYWORDS_DAYS_BACK", "3"))
TOP_K = int(os.getenv("SOCIAL_KEYWORDS_TOPK", "30"))
MIN_DF = int(os.getenv("SOCIAL_KEYWORDS_MIN_DF", "2"))
MAX_DF = float(os.getenv("SOCIAL_KEYWORDS_MAX_DF", "0.70"))

MIN_TOKEN_LEN = int(os.getenv("SOCIAL_KEYWORDS_MIN_TOKEN_LEN", "3"))

RE_TOKEN_OK = re.compile(r"^[A-Za-zÀ-ÿ\u0600-\u06FF][A-Za-zÀ-ÿ\u0600-\u06FF0-9_\-']+$", re.UNICODE)
RE_DIGITS = re.compile(r"^\d+$")


def connect_db() -> PGConnection: 
    if not DB_URL:
        raise RuntimeError("DATABASE_URL introuvable (.env)")
    return psycopg2.connect(DB_URL)


def ensure_nltk():
    try:
        _ = nltk_stopwords.words("french")
    except LookupError:
        nltk.download("stopwords")


# --------------------------
# STRICT NOISE FILTERING (comme France24)
# --------------------------

STOP_FR = set(nltk_stopwords.words("french")) | {
    "france", "français", "française", "actualite", "actualité", "actu", "info", "infos"
}
STOP_EN = set(nltk_stopwords.words("english")) | {
    "france", "news", "breaking", "latest"
}
STOP_ES = set(nltk_stopwords.words("spanish")) | {
    "francia", "noticias", "última", "ultima", "hora"
}
STOP_AR = set(nltk_stopwords.words("arabic")) | {
    "فرنسا", "أخبار"
}

# bruit social fréquent
SOCIAL_NOISE = {
    "reddit", "subreddit", "comments", "comment", "post", "thread", "fil", "lien",
    "https", "http", "www", "amp", "utm", "source", "medium", "campaign",
    "weekend", "semaine", "archives", "automoderator", "modérateur", "moderator"
}

GENERIC_VERBS = {
    # FR
    "être", "avoir", "faire", "dire", "aller", "venir", "pouvoir", "devoir",
    # EN
    "be", "have", "do", "say", "go", "make", "get",
    # ES
    "ser", "estar", "haber", "tener", "decir", "hacer",
    # AR
    "كان", "يكون", "قال"
}

TEMPORAL_WORDS = {
    # FR jours/mois
    "lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche",
    "janvier","février","fevrier","mars","avril","mai","juin","juillet","août","aout",
    "septembre","octobre","novembre","décembre","decembre",
    "an","ans","année","annee","mois",
    # EN
    "monday","tuesday","wednesday","thursday","friday","saturday","sunday",
    "january","february","march","april","may","june","july","august",
    "september","october","november","december","year","years","month","months",
    # ES
    "lunes","martes","miércoles","miercoles","jueves","viernes","sábado","sabado","domingo",
    "enero","febrero","marzo","abril","mayo","junio","julio","agosto",
    "septiembre","octubre","noviembre","diciembre","año","anos","mes","meses",
    # AR (minimal)
    "الاثنين","الثلاثاء","الأربعاء","الخميس","الجمعة","السبت","الأحد","عام","عاما"
}

GENERIC_WORDS = {
    "plus","après","apres","avant","pendant","depuis",
    "while","during","since",
    "mientras","tras","durante",
    "خلال"
}

LANG_STOPWORDS = {
    "fr": STOP_FR,
    "en": STOP_EN,
    "es": STOP_ES,
    "ar": STOP_AR,
}

DISCOURSE_WORDS = {
    # FR
    "jamais", "toujours", "souvent", "parfois",
    "tout", "tous", "toute", "toutes",
    "rien", "quelque", "quelques",
    "cest", "c'est", "ceci", "cela", "ça",
    "fait", "faire", "faites",
    "trouver", "trouvé", "trouve",
    "savoir", "peut", "peux", "peuvent",
    "bien", "mal", "mieux", "bon", "bonne",
    "grand", "grande", "petit", "petite",

    # EN
    "always", "never", "everything", "nothing",
    "something", "anything",
    "thing", "things", "stuff",
    "make", "made", "find", "found",

    # ES
    "siempre", "nunca", "todo", "todos",
    "hacer", "hecho", "encontrar",

    # AR (minimal mais utile)
    "كل", "دائما", "أبدا", "شيء", "أشياء"
}



SOCIAL_GENERIC = {
    "bonjour", "salut", "merci",
    "video", "vidéo", "post", "posts",
    "commentaire", "commentaires",
    "thread", "fil",
    "titre", "lien",
    "reddit", "subreddit",   "oui","non","ici","là","la","ça","ceci","cela",
  "veux","vouloir","veut","voulait","voulant",
  "savoir","passer","pense","penser",
  "également","trop","assez",
  "combien","attention",
  "généré","automatiquement", "généré automatiquement"
}
META_WORDS = {
    "monde", "gens", "personnes",
    "sujet", "sujets",
    "type", "types",
    "cas", "exemple",
}



DEFAULT_STOPWORDS = STOP_FR


def strict_reject(token: str, stopset: set) -> bool:
    if not token:
        return True

    w = token.lower().strip()

    if len(w) < MIN_TOKEN_LEN:
        return True
    if RE_DIGITS.match(w):
        return True
    if any(ch.isdigit() for ch in w):
        return True
    if (
        w in stopset
        or w in DISCOURSE_WORDS
        or w in SOCIAL_GENERIC
        or w in META_WORDS
        or w in SOCIAL_NOISE
        or w in GENERIC_VERBS
        or w in TEMPORAL_WORDS
        or w in GENERIC_WORDS
    ):
        return True
    if not RE_TOKEN_OK.match(w):
        return True

    return False


def fetch_docs(
    conn: PGConnection,
    start_date: dt.date,
    end_date: dt.date,
) -> list[tuple[dt.date, str, str, str]]:
    """
    On récupère le texte propre + lang + la date via raw.published_at.
    """
    sql = """
        SELECT
          (r.published_at AT TIME ZONE 'UTC')::date AS d,
          c.platform, c.source, c.lang,
          COALESCE(NULLIF(c.clean_text, ''), NULLIF(c.title, '')) AS text
        FROM social_posts_clean c
        JOIN social_posts_raw r
          ON r.platform = c.platform AND r.external_id = c.external_id
        WHERE (r.published_at AT TIME ZONE 'UTC')::date >= %s
          AND (r.published_at AT TIME ZONE 'UTC')::date <= %s
          AND COALESCE(NULLIF(c.clean_text, ''), NULLIF(c.title, '')) IS NOT NULL;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start_date, end_date))
        return cur.fetchall()


def build_vectorizer(lang: str, stopset: set[str]) -> TfidfVectorizer:
    def tok(text: str):
        if not text:
            return []
        parts = re.findall(r"[\w\u0600-\u06FF']+", text.lower(), flags=re.UNICODE)
        out = []
        for t in parts:
            if strict_reject(t, stopset):
                continue
            out.append(t)
        return out

    return TfidfVectorizer(
        tokenizer=tok,
        preprocessor=None,
        token_pattern=None,
        lowercase=True,
        min_df=MIN_DF,
        max_df=MAX_DF,
        ngram_range=(1, 2),
        sublinear_tf=True,
        norm="l2"
    )


def upsert_keywords(
    conn: PGConnection,
    rows: Sequence[tuple[dt.date, str, str, str, float]],
) -> None:
    sql = """
      INSERT INTO social_keywords_daily (date, platform, source, lang, keyword, score, n_docs)
      VALUES %s
      ON CONFLICT (date, platform, source, lang, keyword)
      DO UPDATE SET
        score = EXCLUDED.score,
        n_docs = EXCLUDED.n_docs;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)


def main() -> None:
    ensure_nltk()

    today = dt.date.today()
    start_date = today - dt.timedelta(days=DAYS_BACK)
    end_date = today

    conn = connect_db()
    try:
        data = fetch_docs(conn, start_date, end_date)
        logging.info("Fetched %s docs (from %s to %s).", len(data), start_date, end_date)

        groups = defaultdict(list)  # (date, platform, source, lang) -> [texts]
        for d, platform, source, lang, text in data:
            groups[(d, platform, source, lang)].append(text)

        all_rows = []
        for (d, platform, source, lang), texts in groups.items():
            if len(texts) < 2:
                continue

            stopset = LANG_STOPWORDS.get(lang, DEFAULT_STOPWORDS)
            vec = build_vectorizer(lang, stopset)

            try:
                X = vec.fit_transform(texts)
            except ValueError:
                continue

            terms = vec.get_feature_names_out()
            scores = X.mean(axis=0).A1
            top_idx = scores.argsort()[::-1][:TOP_K]
            n_docs = len(texts)

            for i in top_idx:
                kw = terms[i]
                sc = float(scores[i])
                if sc <= 0:
                    continue
                # sécurité : refiltrer (ngram)
                if any(strict_reject(part, stopset) for part in kw.split()):
                    continue
                all_rows.append((d, platform, source, lang, kw, sc, n_docs))

        if all_rows:
            upsert_keywords(conn, all_rows)
            conn.commit()
            logging.info("Upserted %s rows into social_keywords_daily.", len(all_rows))
        else:
            logging.info("No keyword rows to upsert.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
