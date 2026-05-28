# dashboard/data_access.py

import os
from datetime import date
from typing import List, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import register_default_jsonb
from dotenv import load_dotenv
import streamlit as st

load_dotenv()

register_default_jsonb(loads=lambda x: x)


def _get_db_url() -> str | None:
    try:
        if "DATABASE_URL" in st.secrets:
            val = st.secrets["DATABASE_URL"]
            if isinstance(val, str) and val.strip():
                return val.strip()
    except Exception:
        pass

    val = os.getenv("DATABASE_URL")
    if isinstance(val, str) and val.strip():
        return val.strip()

    return None


def get_connection():
    db_url = _get_db_url()
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL non défini. "
            "Local: définir DATABASE_URL dans .env. "
            "Streamlit Cloud: Manage app → Settings → Secrets → DATABASE_URL"
        )

    conn = st.session_state.get("_db_conn")
    if conn is None or getattr(conn, "closed", 1) != 0:
        conn = psycopg2.connect(db_url)
        st.session_state["_db_conn"] = conn

    return conn


def _read_table(table_name: str, columns: str = "*") -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(f"SELECT {columns} FROM {table_name};", conn)


@st.cache_data(ttl=3600)
def get_available_dates() -> List[date]:
    dates = []

    for table, col in [("keywords_daily", "date"), ("topics_daily", "date")]:
        try:
            df = _read_table(table, col)
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                dates.extend(df[col].dt.date.dropna().tolist())
        except Exception:
            pass

    try:
        df = _read_table("articles_raw", "published_at")
        if "published_at" in df.columns:
            df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
            dates.extend(df["published_at"].dt.date.dropna().tolist())
    except Exception:
        pass

    return sorted(set(dates)) if dates else []


@st.cache_data(ttl=3600)
def get_sources(media_type: Optional[str] = None) -> List[str]:
    conn = get_connection()
    params: list = []
    where = ["source IS NOT NULL"]

    if media_type:
        where.append("media_type = %s")
        params.append(media_type)

    query = f"SELECT DISTINCT source FROM articles_raw WHERE {' AND '.join(where)} ORDER BY source;"
    df = pd.read_sql_query(query, conn, params=params or None)
    return ["ALL"] + df["source"].dropna().tolist()


@st.cache_data(ttl=900)
def load_keywords_for_day(
    selected_date: date,
    selected_source: str = "ALL",
    media_type: Optional[str] = None,
) -> pd.DataFrame:
    conn = get_connection()
    query = "SELECT source, media_type, word, count, rank FROM keywords_daily WHERE date = %s"
    params: List = [selected_date]

    if selected_source != "ALL":
        query += " AND source = %s"
        params.append(selected_source)
    if media_type:
        query += " AND media_type = %s"
        params.append(media_type)

    query += " ORDER BY rank ASC, source ASC;"
    return pd.read_sql_query(query, conn, params=params)


@st.cache_data(ttl=900)
def load_keywords_range(
    start_date: date,
    end_date: date,
    media_type: str = "tv",
) -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT date, source, media_type, word, SUM(count) AS total_count
        FROM keywords_daily
        WHERE date BETWEEN %s AND %s
    """
    params = [start_date, end_date]

    if media_type:
        query += " AND media_type = %s"
        params.append(media_type)

    query += " GROUP BY date, source, media_type, word ORDER BY date ASC, source ASC, total_count DESC;"
    df = pd.read_sql_query(query, conn, params=params)

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
    return df


@st.cache_data(ttl=1800)
def load_lemmas_range(
    start_date: date,
    end_date: date,
    media_type: str = "tv",
) -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT ar.published_at::date AS date, ar.source, ar.media_type, ac.lemmas
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

    df = df.explode("lemmas").rename(columns={"lemmas": "lemma"})
    df["lemma"] = df["lemma"].astype(str).str.lower()
    df = df.dropna(subset=["lemma"])

    return df.groupby(["source", "media_type", "lemma"]).size().reset_index(name="total_count")


@st.cache_data(ttl=900)
def load_topics_range(
    start_date: date,
    end_date: date,
    media_type: str = "tv",
    top_n: int = 20,
) -> pd.DataFrame:
    """
    Topics agrégés sur une période : somme des articles par topic_label,
    plus le nombre de jours où le topic a été actif.
    """
    conn = get_connection()
    query = """
        SELECT
            topic_label,
            SUM(articles_count) AS total_articles,
            COUNT(DISTINCT date)  AS days_active,
            MIN(date)             AS first_seen,
            MAX(date)             AS last_seen
        FROM topics_daily
        WHERE date BETWEEN %s AND %s
          AND media_type = %s
          AND source = 'ALL'
          AND topic_label IS NOT NULL
          AND topic_label <> ''
        GROUP BY topic_label
        ORDER BY total_articles DESC
        LIMIT %s;
    """
    df = pd.read_sql_query(query, conn, params=[start_date, end_date, media_type, top_n])
    if not df.empty:
        df["first_seen"] = pd.to_datetime(df["first_seen"]).dt.date
        df["last_seen"] = pd.to_datetime(df["last_seen"]).dt.date
    return df


@st.cache_data(ttl=900)
def load_topics_for_day(selected_date: date, only_tv: bool = True) -> pd.DataFrame:
    conn = get_connection()
    query = "SELECT date, source, media_type, topic_id, topic_label, articles_count, keywords FROM topics_daily WHERE date = %s"
    params: List = [selected_date]

    if only_tv:
        query += " AND media_type = 'tv' AND source = 'ALL'"

    query += " ORDER BY topic_id ASC;"
    return pd.read_sql_query(query, conn, params=params)


@st.cache_data(ttl=900)
def load_topics_timeseries(
    start_date: date,
    end_date: date,
    media_type: str = "tv",
) -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT date, source, media_type, SUM(articles_count) AS total_articles
        FROM topics_daily
        WHERE date BETWEEN %s AND %s AND media_type = %s
        GROUP BY date, source, media_type
        ORDER BY date ASC, source ASC;
    """
    df = pd.read_sql_query(query, conn, params=[start_date, end_date, media_type])

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
    return df


@st.cache_data(ttl=900)
def load_word_trend(
    word: str,
    start_date: date,
    end_date: date,
    media_type: str = "tv",
) -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT date, source, media_type, SUM(count) AS total_mentions
        FROM keywords_daily
        WHERE date BETWEEN %s AND %s AND word = %s
    """
    params: List = [start_date, end_date, word]

    if media_type:
        query += " AND media_type = %s"
        params.append(media_type)

    query += " GROUP BY date, source, media_type ORDER BY date ASC, source ASC;"
    df = pd.read_sql_query(query, conn, params=params)

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
    return df


@st.cache_data(ttl=1800)
def load_narrative_clusters() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(
        "SELECT cluster_id, label, top_keywords, size, created_at FROM narratives_clusters ORDER BY size DESC;",
        conn,
    )


@st.cache_data(ttl=1800)
def load_narrative_distribution_by_source() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(
        """
        SELECT na.cluster_id, ar.source, COUNT(*) AS article_count
        FROM narratives_assignments na
        JOIN articles_raw ar ON ar.id = na.article_id
        GROUP BY na.cluster_id, ar.source
        ORDER BY article_count DESC;
        """,
        conn,
    )
