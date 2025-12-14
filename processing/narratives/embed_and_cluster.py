# processing/narratives/embed_and_cluster.py

import os
import logging
from collections import Counter

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from sentence_transformers import SentenceTransformer
from sklearn.cluster import MiniBatchKMeans
import re
from collections import Counter
from core.db_types import PGConnection


load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# ---------------- Connexion DB ----------------

def get_conn() -> PGConnection:
    if not DB_URL:
        raise RuntimeError("DATABASE_URL manquant dans l'environnement")
    return psycopg2.connect(DB_URL)


# ---------------- Chargement des articles ----------------

def fetch_articles_for_narratives(conn: PGConnection) -> pd.DataFrame:
    """
    Récupère les articles (titre + résumé + lemmas) pour construire les narratifs.
    On joint articles_raw et articles_clean.
    """
    query = """
        SELECT
            ar.id AS article_id,
            ar.source,
            ar.media_type,
            ar.published_at::date AS date,
            ar.title,
            ar.summary,
            ac.lemmas
        FROM articles_raw ar
        JOIN articles_clean ac ON ac.article_id = ar.id
        WHERE ar.published_at IS NOT NULL
        ORDER BY ar.published_at ASC;
    """
    df = pd.read_sql_query(query, conn)
    logging.info(f"{len(df)} articles chargés pour les narratifs.")
    return df


# ---------------- Embeddings ----------------

def build_text_column(df: pd.DataFrame) -> pd.Series:
    """
    Construit un texte d'entrée pour les embeddings :
    Titre + résumé (déjà nettoyé en amont).
    """
    def _combine(row):
        parts = []
        if row["title"]:
            parts.append(str(row["title"]))
        if row["summary"]:
            parts.append(str(row["summary"]))
        return ". ".join(parts)

    return df.apply(_combine, axis=1)


def compute_embeddings(texts: list[str], model_name: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2") -> np.ndarray:
    """
    Calcule les embeddings à partir d'un modèle Sentence-Transformers.
    Modèle multilingue adapté au français.
    """
    logging.info(f"Chargement du modèle d'embeddings : {model_name}")
    model = SentenceTransformer(model_name)

    logging.info("Calcul des embeddings...")
    embeddings = model.encode(
        texts,
        batch_size=64,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    logging.info(f"Embeddings calculés : shape = {embeddings.shape}")
    return embeddings


# ---------------- Clustering ----------------

def cluster_embeddings(
    embeddings: np.ndarray,
    n_clusters: int = 12,
    random_state: int = 42,
) -> np.ndarray:
    """
    Clusterisation des embeddings avec MiniBatchKMeans.
    n_clusters peut être ajusté dans une future config YAML.
    """
    logging.info(f"Lancement du clustering MiniBatchKMeans (n_clusters={n_clusters})...")
    kmeans = MiniBatchKMeans(
        n_clusters=n_clusters,
        random_state=random_state,
        batch_size=256,
        max_iter=200,
    )
    labels = kmeans.fit_predict(embeddings)
    logging.info("Clustering terminé.")
    return labels


FRENCH_STOP_LEMMAS = {
    # Articles / déterminants
    "le", "la", "les", "un", "une", "des", "du", "au", "aux", "ce", "cet",
    "cette", "ces", "tout", "toute", "tous", "toutes",

    # Pronoms personnels / relatifs / démonstratifs / possessifs
    "je", "tu", "il", "elle", "on", "nous", "vous", "ils", "elles",
    "moi", "toi", "lui", "eux", "soi",
    "me", "te", "se", "y", "en", "leur", "leurs",
    "qui", "que", "quoi", "dont", "où", "quel", "quelle", "quels", "quelles",
    "ceci", "cela", "ça", "ci", "là",
    "mon", "ma", "mes", "ton", "ta", "tes", "son", "sa", "ses",

    # Verbes auxiliaires et verbes très fréquents
    "être", "avoir", "faire", "aller", "pouvoir", "devoir", "falloir",
    "venir", "voir", "savoir", "dire",

    # Prépositions / conjonctions / adverbes grammaticaux
    "de", "du", "des", "à", "au", "aux", "dans", "sur", "sous", "chez",
    "entre", "par", "pour", "avec", "sans", "vers", "contre", "chez",
    "avant", "après", "pendant", "depuis", "jusque", "jusqu",
    "et", "ou", "or", "ni", "car", "donc", "mais",
    "si", "comme", "lorsque", "quand", "puisque", "quoique",
    "parce", "que", "afin", "afin_de",

    # Négations et petits adverbes fréquents
    "ne", "pas", "plus", "moins", "très", "trop", "assez",
    "bien", "mal", "encore", "toujours", "jamais", "déjà",
    "aussi", "autre", "autres", "même", "seul", "seule",
    "aucun", "aucune",

    # Mots très génériques inutiles pour les thèmes
    "année", "jour", "fois", "temps", "cas", "chose", "personne",
}

# Stopwords anglais basiques (en cas d'articles en VO)
EN_STOP_LEMMAS = {
    "the", "a", "an", "of", "to", "in", "on", "at", "for", "from", "by",
    "and", "or", "but", "if", "then", "than", "that", "this", "these", "those",
    "be", "have", "do", "is", "are", "was", "were", "been", "being",
    "it", "its", "they", "them", "their", "we", "you",
}

EXTRA_STOP = {
    # jours
    "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche",

    # mois
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",

    # adverbes journalistiques
    "selon", "comment", "voici", "ainsi",

    # adjectifs/adverbes trop généraux
    "nouveau", "nouvelle", "dernier", "dernière", "premier", "première",

    # quantités
    "million", "milliard",

    # mots trop génériques
    "france", "français", "monde", "national",
}



ALL_STOP_LEMMAS = FRENCH_STOP_LEMMAS | EN_STOP_LEMMAS | EXTRA_STOP

PUNCT_REGEX = re.compile(r"^[\W_]+$", re.UNICODE)


def _is_valid_lemma(lemma: str) -> bool:
    if not lemma:
        return False
    lemma = lemma.lower().strip()

    # uniquement lettres (pas de chiffres ni symboles)
    if not lemma.isalpha():
        return False

    # longueur minimale
    if len(lemma) <= 2:
        return False

    # stopwords (grammaticaux, ultra fréquents)
    if lemma in ALL_STOP_LEMMAS:
        return False

    # pure ponctuation (par sécurité)
    if PUNCT_REGEX.match(lemma):
        return False

    return True



def build_cluster_summaries(df: pd.DataFrame, labels: np.ndarray, top_k: int = 10) -> pd.DataFrame:
    """
    Pour chaque cluster :
    - taille
    - top lemmas nettoyés (sans stopwords)
    - label textuel basé sur ces lemmas
    """
    df = df.copy()
    df["cluster_id"] = labels

    rows = []

    for cluster_id, group in df.groupby("cluster_id"):
        size = len(group)

        # Lemmas agrégés sur le cluster
        all_lemmas = []
        for lemmas in group["lemmas"]:
            if isinstance(lemmas, list):
                for l in lemmas:
                    l = str(l).lower()
                    if _is_valid_lemma(l):
                        all_lemmas.append(l)

       # Compter les occurrences
        counter = Counter(all_lemmas)
        total = sum(counter.values())

        top_lemmas: list[str] = []

        if total > 0:
            # Supprimer les mots trop fréquents (si > 40% du cluster)
            filtered_lemmas = []
            for lemma, freq in counter.items():
                ratio = freq / total
                if ratio >= 0.40:
                    continue  # bruit trop dominant
                filtered_lemmas.extend([lemma] * freq)

            # Recalculer après filtrage
            counter = Counter(filtered_lemmas)
            top_lemmas = [w for w, _ in counter.most_common(top_k)]



        # Label simplifié = concat de 3–4 lemmes principaux
        label = ", ".join(top_lemmas[:4]) if top_lemmas else f"Cluster {cluster_id}"

        rows.append(
            {
                "cluster_id": int(cluster_id),
                "label": label,
                "top_keywords": top_lemmas,
                "size": int(size),
            }
        )

    clusters_df = pd.DataFrame(rows)
    logging.info(f"{len(clusters_df)} clusters résumés.")
    return df, clusters_df



# ---------------- Écriture en base ----------------

def reset_narratives_tables(conn: PGConnection) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE narratives_assignments CASCADE;")
        cur.execute("TRUNCATE TABLE narratives_clusters CASCADE;")
    conn.commit()
    logging.info("Tables narratives_* vidées.")


def insert_clusters(conn: PGConnection, clusters_df: pd.DataFrame) -> None:
    records = [
        (row["cluster_id"], row["label"], row["top_keywords"], row["size"])
        for _, row in clusters_df.iterrows()
    ]

    query = """
        INSERT INTO narratives_clusters (cluster_id, label, top_keywords, size)
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, query, records)
    conn.commit()
    logging.info(f"{len(records)} lignes insérées dans narratives_clusters.")


def insert_assignments(conn: PGConnection, df_with_clusters: pd.DataFrame) -> None:
    records = [
        (int(row["article_id"]), int(row["cluster_id"]), None)
        for _, row in df_with_clusters.iterrows()
    ]

    query = """
        INSERT INTO narratives_assignments (article_id, cluster_id, score)
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, query, records)
    conn.commit()
    logging.info(f"{len(records)} lignes insérées dans narratives_assignments.")


# ---------------- Main pipeline ----------------

def main() -> None:
    conn = get_conn()

    try:
        df = fetch_articles_for_narratives(conn)
        if df.empty:
            logging.warning("Aucun article disponible pour le clustering.")
            return

        texts = build_text_column(df)
        embeddings = compute_embeddings(texts)

        labels = cluster_embeddings(embeddings, n_clusters=12)

        df_with_clusters, clusters_df = build_cluster_summaries(df, labels, top_k=12)

        reset_narratives_tables(conn)
        insert_clusters(conn, clusters_df)
        insert_assignments(conn, df_with_clusters)

        logging.info("Pipeline narratifs (embeddings + clustering) terminé avec succès.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
