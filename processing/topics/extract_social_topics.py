import os
import re
import json
import logging
import datetime as dt
from collections import defaultdict

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF

import nltk
from nltk.corpus import stopwords as nltk_stopwords
from typing import Any
from core.db_types import PGConnection
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_URL = os.getenv("DATABASE_URL")

DAYS_BACK = int(os.getenv("SOCIAL_TOPICS_DAYS_BACK", "3"))
DEFAULT_N_TOPICS = int(os.getenv("SOCIAL_TOPICS_N", "8"))
TOP_TERMS = int(os.getenv("SOCIAL_TOPICS_TOP_TERMS", "12"))
MIN_DF = int(os.getenv("SOCIAL_TOPICS_MIN_DF", "2"))
MAX_DF = float(os.getenv("SOCIAL_TOPICS_MAX_DF", "0.80"))
MIN_TOKEN_LEN = int(os.getenv("SOCIAL_TOPICS_MIN_TOKEN_LEN", "3"))

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


# --- STRICT FILTER (même logique que keywords) ---
STOP_FR = set(nltk_stopwords.words("french")) | {"france","français","française","actualité","actu","info","infos"}
STOP_EN = set(nltk_stopwords.words("english")) | {"france","news","breaking","latest"}
STOP_ES = set(nltk_stopwords.words("spanish")) | {"francia","noticias","última","ultima","hora"}
STOP_AR = set(nltk_stopwords.words("arabic")) | {"فرنسا","أخبار"}

SOCIAL_NOISE = {"reddit","subreddit","comments","comment","post","thread","fil","lien","https","http","www","amp","utm","source","medium","campaign",
                "weekend","semaine","archives","automoderator","moderator"}

GENERIC_VERBS = {"être","avoir","faire","dire","aller","venir","pouvoir","devoir",
                 "be","have","do","say","go","make","get",
                 "ser","estar","haber","tener","decir","hacer",
                 "كان","يكون","قال"}

TEMPORAL_WORDS = {
    "lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche",
    "janvier","février","fevrier","mars","avril","mai","juin","juillet","août","aout",
    "septembre","octobre","novembre","décembre","decembre","an","ans","année","annee","mois",
    "monday","tuesday","wednesday","thursday","friday","saturday","sunday",
    "january","february","march","april","may","june","july","august","september","october","november","december","year","years","month","months",
    "lunes","martes","miércoles","miercoles","jueves","viernes","sábado","sabado","domingo",
    "enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre","año","anos","mes","meses",
    "الاثنين","الثلاثاء","الأربعاء","الخميس","الجمعة","السبت","الأحد","عام","عاما"
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


GENERIC_WORDS = {"plus","après","apres","avant","pendant","depuis","while","during","since","mientras","tras","durante","خلال"}

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


LANG_STOPWORDS = {"fr": STOP_FR, "en": STOP_EN, "es": STOP_ES, "ar": STOP_AR}
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


def topics_n_for_lang(lang: str) -> int:
    key = f"SOCIAL_TOPICS_N_{lang.upper()}"
    return int(os.getenv(key, str(DEFAULT_N_TOPICS)))


def fetch_docs(conn: PGConnection, start_date: dt.date, end_date: dt.date) -> list[tuple[dt.date, str, str, str]]:
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


def main() -> None: 
    ensure_nltk()

    today = dt.date.today()
    start_date = today - dt.timedelta(days=DAYS_BACK)
    end_date = today

    conn = connect_db()
    try:
        data = fetch_docs(conn, start_date, end_date)
        logging.info("Fetched %s docs (from %s to %s).", len(data), start_date, end_date)

        groups = defaultdict(list)
        for d, platform, source, lang, text in data:
            groups[(d, platform, source, lang)].append(text)

        rows = []

        for (d, platform, source, lang), texts in groups.items():
            # NMF needs some minimum volume to be meaningful
            if len(texts) < 10:
                continue

            stopset = LANG_STOPWORDS.get(lang, DEFAULT_STOPWORDS)
            vec = build_vectorizer(lang, stopset)

            # 1) Vectorize
            try:
                X = vec.fit_transform(texts)
            except ValueError:
                # e.g., empty vocabulary after strict filtering
                continue

            n_samples, n_features = X.shape
            max_components = min(n_samples, n_features)

            # If too few features or docs, skip
            if max_components < 2:
                continue

            # 2) Choose safe topic count (never > min(n_samples, n_features))
            # - desired: topics_n_for_lang
            # - heuristic: <= n_samples//3 to avoid overfitting
            desired = topics_n_for_lang(lang)
            heuristic = max(2, n_samples // 3)
            n_topics = min(desired, heuristic, max_components)

            if n_topics < 2:
                continue

            # 3) Fit NMF (safe now)
            try:
                nmf = NMF(
                    n_components=n_topics,
                    init="nndsvda",
                    random_state=42,
                    max_iter=400
                )
                W = nmf.fit_transform(X)
            except ValueError:
                # Extremely rare edge cases (numerical issues)
                continue

            H = nmf.components_
            terms = vec.get_feature_names_out()

            topic_weights = W.mean(axis=0)
            n_docs = len(texts)

            for topic_id in range(n_topics):
                comp = H[topic_id]
                top_idx = comp.argsort()[::-1][:TOP_TERMS]
                top_terms = [terms[i] for i in top_idx]

                # sécurité: refiltrer les bigrams
                top_terms = [t for t in top_terms if not any(strict_reject(p, stopset) for p in t.split())]

                rows.append((
                    d, platform, source, lang,
                    int(topic_id),
                    json.dumps(top_terms),
                    float(topic_weights[topic_id]),
                    int(n_docs)
                ))

        if not rows:
            logging.info("No topic rows to upsert.")
            return

        sql = """
          INSERT INTO social_topics_daily (date, platform, source, lang, topic_id, top_terms, weight, n_docs)
          VALUES %s
          ON CONFLICT (date, platform, source, lang, topic_id)
          DO UPDATE SET
            top_terms = EXCLUDED.top_terms,
            weight = EXCLUDED.weight,
            n_docs = EXCLUDED.n_docs;
        """

        with conn.cursor() as cur:
            execute_values(
                cur,
                sql,
                rows,
                template="(%s,%s,%s,%s,%s,%s::jsonb,%s,%s)",
                page_size=200
            )

        conn.commit()
        logging.info("Upserted %s topic rows into social_topics_daily.", len(rows))

    finally:
        conn.close()



if __name__ == "__main__":
    main()
