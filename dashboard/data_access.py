# dashboard/data_access.py

import os
from datetime import date
from typing import List, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import register_default_jsonb
from dotenv import load_dotenv
import streamlit as st

# Charger les variables d'environnement (DATABASE_URL)
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

# Pour que les colonnes JSONB soient bien interprétées par psycopg2/pandas
register_default_jsonb(loads=lambda x: x)


# ---------- Connexion DB ----------


def get_connection():
    """
    Connexion PostgreSQL réutilisée dans la session Streamlit.
    Si la connexion est fermée (timeout / rerun), on la recrée.
    """
    if not DB_URL:
        raise RuntimeError("DATABASE_URL non défini dans .env")

    conn = st.session_state.get("_db_conn")

    # psycopg2: closed == 0 => ouverte ; closed != 0 => fermée
    if conn is None or getattr(conn, "closed", 1) != 0:
        conn = psycopg2.connect(DB_URL)
        st.session_state["_db_conn"] = conn

    return conn



# ---------- Helper générique ----------

def _read_table(table_name: str, columns: str = "*") -> pd.DataFrame:
    """
    Lit une table PostgreSQL dans un DataFrame pandas.
    """
    conn = get_connection()
    query = f"SELECT {columns} FROM {table_name};"
    return pd.read_sql_query(query, conn)


# ---------- Dates & sources ----------

@st.cache_data
def get_available_dates() -> List[date]:
    """
    Retourne toutes les dates disponibles (union) provenant :
    - keywords_daily.date
    - topics_daily.date
    - articles_raw.published_at (converti en date)
    """
    dates = []

    # keywords_daily.date
    try:
        df_kw = _read_table("keywords_daily", "date")
        if "date" in df_kw.columns:
            df_kw["date"] = pd.to_datetime(df_kw["date"], errors="coerce")
            dates.extend(df_kw["date"].dt.date.dropna().tolist())
    except Exception as e:
        print("Erreur lecture keywords_daily:", e)

    # topics_daily.date
    try:
        df_tp = _read_table("topics_daily", "date")
        if "date" in df_tp.columns:
            df_tp["date"] = pd.to_datetime(df_tp["date"], errors="coerce")
            dates.extend(df_tp["date"].dt.date.dropna().tolist())
    except Exception as e:
        print("Erreur lecture topics_daily:", e)

    # articles_raw.published_at
    try:
        df_raw = _read_table("articles_raw", "published_at")
        if "published_at" in df_raw.columns:
            df_raw["published_at"] = pd.to_datetime(df_raw["published_at"], errors="coerce")
            dates.extend(df_raw["published_at"].dt.date.dropna().tolist())
    except Exception as e:
        print("Erreur lecture articles_raw:", e)

    if not dates:
        return []

    # dates uniques triées
    return sorted(sorted(set(dates)))


@st.cache_data
def get_sources(media_type: Optional[str] = None) -> List[str]:
    """
    Retourne la liste des sources disponibles à partir de articles_raw.

    - Si media_type est None  -> tous types de médias (tv + press).
    - Si media_type = 'tv'    -> seulement les chaînes TV.
    - Si media_type = 'press' -> seulement la presse écrite.
    """
    conn = get_connection()

    # Construction de la requête de façon robuste
    params: list = []
    where_clauses = ["source IS NOT NULL"]

    if media_type:
        where_clauses.append("media_type = %s")
        params.append(media_type)

    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT DISTINCT source
        FROM articles_raw
        WHERE {where_sql}
        ORDER BY source;
    """

    df = pd.read_sql_query(query, conn, params=params or None)

    sources = df["source"].dropna().tolist()
    # On préfixe par ALL pour l'interface
    return ["ALL"] + sources



# ---------- Chargement des mots-clés ----------

@st.cache_data
def load_keywords_for_day(
    selected_date: date,
    selected_source: str = "ALL",
    media_type: Optional[str] = None,
) -> pd.DataFrame:
    """
    Top mots-clés pour une date donnée, éventuellement filtrés par source & media_type.
    """
    conn = get_connection()

    base_query = """
        SELECT source, media_type, word, count, rank
        FROM keywords_daily
        WHERE date = %s
    """
    params: List = [selected_date]

    if selected_source != "ALL":
        base_query += " AND source = %s"
        params.append(selected_source)

    if media_type:
        base_query += " AND media_type = %s"
        params.append(media_type)

    base_query += " ORDER BY rank ASC, source ASC;"

    df = pd.read_sql_query(base_query, conn, params=params)
    return df


@st.cache_data
def load_keywords_range(
    start_date: date,
    end_date: date,
    media_type: str = "tv",
) -> pd.DataFrame:
    """
    Agrège les mots-clés sur une période :
    somme des counts par (date, source, media_type, word).
    Sert à l'analyse de narratifs et au radar 'media bias'.
    """
    conn = get_connection()
    query = """
        SELECT
            date,
            source,
            media_type,
            word,
            SUM(count) AS total_count
        FROM keywords_daily
        WHERE date BETWEEN %s AND %s
    """
    params = [start_date, end_date]

    if media_type:
        query += " AND media_type = %s"
        params.append(media_type)

    query += """
        GROUP BY date, source, media_type, word
        ORDER BY date ASC, source ASC, total_count DESC;
    """

    df = pd.read_sql_query(query, conn, params=params)

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

    return df


@st.cache_data
def load_lemmas_range(
    start_date: date,
    end_date: date,
    media_type: str = "tv",
) -> pd.DataFrame:
    """
    Agrège les lemmes sur une période :
    compte de chaque lemme par source (et type média).
    Utilisé pour l'analyse des narratifs & radar 'media bias'.
    """
    conn = get_connection()

    query = """
        SELECT
            ar.published_at::date AS date,
            ar.source,
            ar.media_type,
            ac.lemmas
        FROM articles_clean ac
        JOIN articles_raw ar ON ac.article_id = ar.id
        WHERE ar.published_at::date BETWEEN %s AND %s
    """
    params = [start_date, end_date]

    if media_type:
        query += " AND ar.media_type = %s"
        params.append(media_type)

    df = pd.read_sql_query(query, conn, params=params)

    if df.empty:
        return df

    # On explose le tableau de lemmes : chaque ligne = (date, source, media_type, lemma)
    df = df.explode("lemmas").rename(columns={"lemmas": "lemma"})
    df["lemma"] = df["lemma"].astype(str).str.lower()
    df = df.dropna(subset=["lemma"])

    # Agrégation : total par source / media_type / lemma
    agg = (
        df.groupby(["source", "media_type", "lemma"])
        .size()
        .reset_index(name="total_count")
    )

    return agg



# ---------- Chargement des topics ----------

@st.cache_data
def load_topics_for_day(selected_date: date, only_tv: bool = True) -> pd.DataFrame:
    """
    Sujets pour une date donnée (topics_daily).
    Par défaut : média TV (media_type = 'tv', source = 'ALL').
    """
    conn = get_connection()
    query = """
        SELECT date, source, media_type, topic_id, topic_label, articles_count, keywords
        FROM topics_daily
        WHERE date = %s
    """
    params: List = [selected_date]

    if only_tv:
        query += " AND media_type = 'tv' AND source = 'ALL'"

    query += " ORDER BY topic_id ASC;"

    df = pd.read_sql_query(query, conn, params=params)
    return df


@st.cache_data
def load_topics_timeseries(
    start_date: date,
    end_date: date,
    media_type: str = "tv",
) -> pd.DataFrame:
    """
    Timeline agrégée des sujets (somme des articles_count par jour et par source).
    Sert pour la heatmap / courbes.
    """
    conn = get_connection()
    query = """
        SELECT
            date,
            source,
            media_type,
            SUM(articles_count) AS total_articles
        FROM topics_daily
        WHERE date BETWEEN %s AND %s
          AND media_type = %s
        GROUP BY date, source, media_type
        ORDER BY date ASC, source ASC;
    """
    df = pd.read_sql_query(query, conn, params=[start_date, end_date, media_type])

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

    return df



# ---------- Tendance d'un mot-clé ----------

@st.cache_data
def load_word_trend(
    word: str,
    start_date: date,
    end_date: date,
    media_type: str = "tv",
) -> pd.DataFrame:
    """
    Évolution d'un mot-clé donné sur une période
    (somme des counts par date et source).
    """
    conn = get_connection()
    query = """
        SELECT
            date,
            source,
            media_type,
            SUM(count) AS total_mentions
        FROM keywords_daily
        WHERE date BETWEEN %s AND %s
          AND word = %s
    """
    params: List = [start_date, end_date, word]

    if media_type:
        query += " AND media_type = %s"
        params.append(media_type)

    query += """
        GROUP BY date, source, media_type
        ORDER BY date ASC, source ASC;
    """

    df = pd.read_sql_query(query, conn, params=params)

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
    return df



@st.cache_data
def load_narrative_clusters() -> pd.DataFrame:
    """
    Charge tous les clusters de narratifs (narratives_clusters).
    """
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT cluster_id, label, top_keywords, size, created_at
        FROM narratives_clusters
        ORDER BY size DESC;
        """,
        conn,
    )
    return df


@st.cache_data
def load_narrative_distribution_by_source() -> pd.DataFrame:
    """
    Distribution des narratifs par chaîne :
    nombre d'articles par (cluster_id, source).
    """
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT
            na.cluster_id,
            ar.source,
            COUNT(*) AS article_count
        FROM narratives_assignments na
        JOIN articles_raw ar ON ar.id = na.article_id
        GROUP BY na.cluster_id, ar.source
        ORDER BY article_count DESC;
        """,
        conn,
    )
    return df
