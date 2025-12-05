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

@st.cache_resource
def get_connection():
    """
    Connexion PostgreSQL réutilisable (cache côté Streamlit).
    """
    if not DB_URL:
        raise RuntimeError("DATABASE_URL non défini dans .env")
    return psycopg2.connect(DB_URL)


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
def get_sources() -> List[str]:
    """
    Retourne la liste des chaînes (sources) présentes dans keywords_daily.
    """
    try:
        df = _read_table("keywords_daily", "DISTINCT source")
        sources = df["source"].dropna().tolist()
    except Exception:
        sources = []

    if "ALL" not in sources:
        sources = ["ALL"] + sources

    return sources


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
