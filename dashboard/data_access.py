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
            COALESCE(llm_label, topic_label) AS topic_label,
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
        GROUP BY COALESCE(llm_label, topic_label)
        ORDER BY total_articles DESC
        LIMIT %s;
    """
    df = pd.read_sql_query(query, conn, params=[start_date, end_date, media_type, top_n])
    if not df.empty:
        df["first_seen"] = pd.to_datetime(df["first_seen"]).dt.date
        df["last_seen"] = pd.to_datetime(df["last_seen"]).dt.date
    return df


@st.cache_data(ttl=900)
def load_agenda_gap(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Joins TV keywords and social keywords on the same word to expose
    topics over-covered by one medium and under-covered by the other.
    Returns: keyword, tv_count, social_score (both normalised 0-1 added as _norm columns).
    """
    conn = get_connection()
    query = """
        WITH tv AS (
            SELECT LOWER(word) AS word, SUM(count) AS tv_count
            FROM keywords_daily
            WHERE date BETWEEN %s AND %s AND media_type = 'tv'
            GROUP BY LOWER(word)
        ),
        social AS (
            SELECT LOWER(keyword) AS word, SUM(score) AS social_score
            FROM social_keywords_daily
            WHERE date BETWEEN %s AND %s
            GROUP BY LOWER(keyword)
        )
        SELECT
            COALESCE(t.word, s.word) AS keyword,
            COALESCE(t.tv_count, 0)      AS tv_count,
            COALESCE(s.social_score, 0)  AS social_score
        FROM tv t
        FULL OUTER JOIN social s USING (word)
        WHERE COALESCE(t.tv_count, 0) + COALESCE(s.social_score, 0) > 0
        ORDER BY tv_count DESC, social_score DESC
        LIMIT 300;
    """
    df = pd.read_sql_query(query, conn, params=[start_date, end_date, start_date, end_date])
    if df.empty:
        return df
    tv_max = df["tv_count"].max() or 1
    soc_max = df["social_score"].max() or 1
    df["tv_norm"]     = df["tv_count"]    / tv_max
    df["social_norm"] = df["social_score"] / soc_max
    return df


@st.cache_data(ttl=1800)
def load_lifecycle(start_date: date, end_date: date, top_n: int = 30) -> pd.DataFrame:
    """
    Topic lifecycle data: first/last seen, peak date, total mentions.
    Falls back to computing from topics_daily if topic_lifetime is empty.
    """
    conn = get_connection()

    # Try the pre-computed lifetime table first
    df = pd.read_sql_query(
        """
        SELECT topic_label,
               first_seen_date AS first_seen,
               last_seen_date  AS last_seen,
               peak_date,
               total_mentions
        FROM topic_lifetime
        WHERE first_seen_date >= %s AND last_seen_date <= %s
           AND topic_label IS NOT NULL AND topic_label <> ''
        ORDER BY total_mentions DESC
        LIMIT %s;
        """,
        conn,
        params=[start_date, end_date, top_n],
    )

    if df.empty:
        # Fallback: compute from topics_daily on the fly
        df = pd.read_sql_query(
            """
            SELECT topic_label,
                   MIN(date) AS first_seen,
                   MAX(date) AS last_seen,
                   (SELECT date FROM topics_daily t2
                    WHERE t2.topic_label = t.topic_label
                    ORDER BY articles_count DESC LIMIT 1) AS peak_date,
                   SUM(articles_count) AS total_mentions
            FROM topics_daily t
            WHERE date BETWEEN %s AND %s
              AND media_type = 'tv' AND source = 'ALL'
              AND topic_label IS NOT NULL AND topic_label <> ''
            GROUP BY topic_label
            ORDER BY total_mentions DESC
            LIMIT %s;
            """,
            conn,
            params=[start_date, end_date, top_n],
        )

    if df.empty:
        return df

    for col in ["first_seen", "last_seen", "peak_date"]:
        df[col] = pd.to_datetime(df[col])

    df["duration_days"] = (df["last_seen"] - df["first_seen"]).dt.days + 1
    return df


@st.cache_data(ttl=900)
def load_topics_for_day(selected_date: date, only_tv: bool = True) -> pd.DataFrame:
    conn = get_connection()
    query = "SELECT date, source, media_type, topic_id, COALESCE(llm_label, topic_label) AS topic_label, articles_count, keywords FROM topics_daily WHERE date = %s"
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
def load_word_trend_fulltext(
    word: str,
    start_date: date,
    end_date: date,
    media_type: Optional[str] = "tv",
) -> pd.DataFrame:
    """
    Article-level trend search that bypasses keywords_daily entirely.
    Searches title + summary in articles_raw using a word-boundary regex so
    morphological variants are included (islam → islamiste, islamiques…).
    Works for any word regardless of whether it made the top_n cut-off.
    """
    conn = get_connection()
    # \m = word start boundary in PostgreSQL regex
    pattern = r"\m" + word.lower()

    query = """
        SELECT
            ar.published_at::date AS date,
            ar.source,
            ar.media_type,
            COUNT(*) AS total_mentions
        FROM articles_raw ar
        WHERE ar.published_at::date BETWEEN %s AND %s
          AND (LOWER(ar.title) ~ %s OR LOWER(COALESCE(ar.summary, '')) ~ %s)
    """
    params: List = [start_date, end_date, pattern, pattern]

    if media_type:
        query += " AND ar.media_type = %s"
        params.append(media_type)

    query += """
        GROUP BY ar.published_at::date, ar.source, ar.media_type
        ORDER BY date ASC, source ASC;
    """
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


@st.cache_data(ttl=900)
def load_ner_entities(
    start_date: date,
    end_date: date,
    media_type: str = "tv",
    entity_labels: Optional[List[str]] = None,
    top_n: int = 50,
) -> pd.DataFrame:
    """
    Top named entities from articles_clean.entities (JSONB array).
    Returns: source, media_type, entity_text, entity_label, mention_count.
    """
    if entity_labels is None:
        entity_labels = ["PER", "ORG", "LOC", "MISC"]

    conn = get_connection()
    label_filter = " AND ent->>'label' = ANY(%s)" if entity_labels else ""

    query = f"""
        SELECT
            ar.source,
            ar.media_type,
            (ent->>'text')  AS entity_text,
            (ent->>'label') AS entity_label,
            COUNT(*)        AS mention_count
        FROM articles_raw ar
        JOIN articles_clean ac ON ac.article_id = ar.id
        CROSS JOIN LATERAL jsonb_array_elements(ac.entities) AS ent
        WHERE ar.published_at::date BETWEEN %s AND %s
          AND ar.media_type = %s
          AND LENGTH(COALESCE(ent->>'text', '')) > 2
          {label_filter}
        GROUP BY ar.source, ar.media_type, entity_text, entity_label
        ORDER BY mention_count DESC
        LIMIT %s;
    """
    params = [start_date, end_date, media_type]
    if entity_labels:
        params.append(entity_labels)
    params.append(top_n)

    df = pd.read_sql_query(query, conn, params=params)
    return df


@st.cache_data(ttl=1800)
def load_entity_trend(
    entity_text: str,
    start_date: date,
    end_date: date,
    media_type: str = "tv",
) -> pd.DataFrame:
    """Daily mention count of a specific entity across sources."""
    conn = get_connection()
    query = """
        SELECT
            ar.published_at::date AS date,
            ar.source,
            COUNT(*) AS mention_count
        FROM articles_raw ar
        JOIN articles_clean ac ON ac.article_id = ar.id
        CROSS JOIN LATERAL jsonb_array_elements(ac.entities) AS ent
        WHERE ar.published_at::date BETWEEN %s AND %s
          AND ar.media_type = %s
          AND LOWER(ent->>'text') = LOWER(%s)
        GROUP BY ar.published_at::date, ar.source
        ORDER BY date ASC, source ASC;
    """
    df = pd.read_sql_query(query, conn, params=[start_date, end_date, media_type, entity_text])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=900)
def load_entity_source_heatmap(
    start_date: date,
    end_date: date,
    media_type: str = "tv",
    entity_label: str = "PER",
    top_n: int = 20,
) -> pd.DataFrame:
    """
    Cross-source entity coverage: which sources mention which entities most.
    Returns: entity_text, source, mention_count.
    """
    conn = get_connection()
    query = """
        WITH top_ents AS (
            SELECT (ent->>'text') AS entity_text, COUNT(*) AS total
            FROM articles_raw ar
            JOIN articles_clean ac ON ac.article_id = ar.id
            CROSS JOIN LATERAL jsonb_array_elements(ac.entities) AS ent
            WHERE ar.published_at::date BETWEEN %s AND %s
              AND ar.media_type = %s
              AND ent->>'label' = %s
              AND LENGTH(COALESCE(ent->>'text', '')) > 2
            GROUP BY entity_text
            ORDER BY total DESC
            LIMIT %s
        )
        SELECT
            te.entity_text,
            ar.source,
            COUNT(*) AS mention_count
        FROM top_ents te
        JOIN articles_clean ac
          ON EXISTS (
              SELECT 1 FROM jsonb_array_elements(ac.entities) ent
              WHERE ent->>'text' = te.entity_text AND ent->>'label' = %s
          )
        JOIN articles_raw ar ON ar.id = ac.article_id
        WHERE ar.published_at::date BETWEEN %s AND %s
          AND ar.media_type = %s
        GROUP BY te.entity_text, ar.source
        ORDER BY te.entity_text, mention_count DESC;
    """
    params = [start_date, end_date, media_type, entity_label, top_n,
              entity_label, start_date, end_date, media_type]
    return pd.read_sql_query(query, conn, params=params)


@st.cache_data(ttl=900)
def count_articles_by_source(
    start_date: date,
    end_date: date,
    media_type: Optional[str] = None,
) -> dict:
    """Return {source: article_count} for the period — used for confidence gating."""
    conn = get_connection()
    query = """
        SELECT source, COUNT(*) AS n
        FROM articles_raw
        WHERE published_at::date BETWEEN %s AND %s
    """
    params: list = [start_date, end_date]
    if media_type:
        query += " AND media_type = %s"
        params.append(media_type)
    query += " GROUP BY source ORDER BY source"
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return dict(zip(df["source"], df["n"].astype(int)))
    except Exception:
        return {}


@st.cache_data(ttl=900)
def load_entity_stance(
    start_date: date,
    end_date: date,
    entity_label: Optional[str] = None,
    top_n: int = 30,
) -> pd.DataFrame:
    """
    Aggregated stance scores from entity_stance_daily.
    Returns: entity_text, entity_label, source, positive_count,
             negative_count, mention_count, net_score.
    net_score ∈ [-1, 1]: positive = favourable coverage, negative = critical.
    """
    conn = get_connection()
    label_clause = "AND entity_label = %s" if entity_label else ""
    query = f"""
        WITH agg AS (
            SELECT
                entity_text,
                entity_label,
                source,
                SUM(positive_count) AS positive_count,
                SUM(negative_count) AS negative_count,
                SUM(mention_count)  AS mention_count
            FROM entity_stance_daily
            WHERE date BETWEEN %s AND %s
              {label_clause}
            GROUP BY entity_text, entity_label, source
        ),
        top_ents AS (
            SELECT entity_text
            FROM agg
            GROUP BY entity_text
            ORDER BY SUM(mention_count) DESC
            LIMIT %s
        )
        SELECT
            a.entity_text,
            a.entity_label,
            a.source,
            a.positive_count,
            a.negative_count,
            a.mention_count,
            ROUND(
                (a.positive_count - a.negative_count)::numeric
                / NULLIF(a.mention_count, 0), 4
            )::float AS net_score
        FROM agg a
        JOIN top_ents t USING (entity_text)
        ORDER BY a.mention_count DESC, a.entity_text;
    """
    params: list = [start_date, end_date]
    if entity_label:
        params.append(entity_label)
    params.append(top_n)
    try:
        return pd.read_sql_query(query, conn, params=params)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=900)
def load_entity_stance_trend(
    entity_text: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Daily net_score trend for a specific entity, broken down by source."""
    conn = get_connection()
    query = """
        SELECT
            date,
            source,
            positive_count,
            negative_count,
            mention_count,
            ROUND(
                (positive_count - negative_count)::numeric
                / NULLIF(mention_count, 0), 4
            )::float AS net_score
        FROM entity_stance_daily
        WHERE LOWER(entity_text) = LOWER(%s)
          AND date BETWEEN %s AND %s
        ORDER BY date ASC, source ASC;
    """
    try:
        df = pd.read_sql_query(query, conn, params=[entity_text, start_date, end_date])
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame()


def _ensure_watchlist_tables(conn) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_terms (
            id       SERIAL PRIMARY KEY,
            term     TEXT NOT NULL UNIQUE,
            added_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS alerts_sent (
            id         BIGSERIAL PRIMARY KEY,
            term       TEXT    NOT NULL,
            alert_date DATE    NOT NULL,
            z_score    FLOAT,
            channel    TEXT    NOT NULL DEFAULT 'telegram',
            sent_at    TIMESTAMP DEFAULT NOW(),
            UNIQUE (term, alert_date, channel)
        );
    """)
    conn.commit()
    cur.close()


def load_watchlist_terms() -> List[str]:
    """Return watchlist terms from DB (no caching — always fresh)."""
    try:
        conn = get_connection()
        _ensure_watchlist_tables(conn)
        cur = conn.cursor()
        cur.execute("SELECT term FROM watchlist_terms ORDER BY term")
        terms = [r[0] for r in cur.fetchall()]
        cur.close()
        return terms
    except Exception:
        return []


def add_watchlist_term(term: str) -> bool:
    """Insert a term. Returns True if inserted, False if duplicate/error."""
    try:
        conn = get_connection()
        _ensure_watchlist_tables(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO watchlist_terms (term) VALUES (%s) ON CONFLICT (term) DO NOTHING",
            (term.strip().lower(),),
        )
        inserted = cur.rowcount > 0
        conn.commit()
        cur.close()
        return inserted
    except Exception:
        return False


def remove_watchlist_term(term: str) -> None:
    """Delete a term from the watchlist."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM watchlist_terms WHERE term = %s", (term,))
        conn.commit()
        cur.close()
    except Exception:
        pass


@st.cache_data(ttl=900)
def load_alert_history(limit: int = 50) -> pd.DataFrame:
    """Return recent sent alerts from alerts_sent."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            """
            SELECT term, alert_date, z_score, channel, sent_at
            FROM alerts_sent
            ORDER BY sent_at DESC
            LIMIT %s
            """,
            conn,
            params=[limit],
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=1800)
def load_weekly_digests(limit: int = 12) -> pd.DataFrame:
    """Return the most recent weekly digests."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            """
            SELECT week_start, week_end, digest_text, context_json, generated_at
            FROM weekly_digests
            ORDER BY week_start DESC
            LIMIT %s
            """,
            conn,
            params=[limit],
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_dashboard_config() -> dict:
    """Load pipeline.yaml — used to read config thresholds in dashboard views."""
    from pathlib import Path
    import yaml
    for parent in Path(__file__).parents:
        candidate = parent / "media_agenda_insights" / "infra" / "config" / "pipeline.yaml"
        if candidate.exists():
            try:
                with open(candidate, encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
            except Exception:
                return {}
    return {}
