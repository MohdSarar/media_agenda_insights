# dashboard/views/social_observable.py

import os
import pandas as pd
import streamlit as st
import psycopg2

from dashboard.ui.components import section_header

DB_URL = os.getenv("DATABASE_URL")


def _conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL introuvable.")
    return psycopg2.connect(DB_URL)


@st.cache_data(ttl=600)
def fetch_distinct_filters():
    conn = _conn()
    try:
        platforms = pd.read_sql("SELECT DISTINCT platform FROM social_posts_raw ORDER BY platform;", conn)
        sources = pd.read_sql("SELECT DISTINCT source FROM social_posts_raw ORDER BY source;", conn)
        langs = pd.read_sql(
            "SELECT DISTINCT lang FROM social_posts_clean WHERE lang IS NOT NULL ORDER BY lang;", conn
        )
        date_bounds = pd.read_sql(
            """
            SELECT
              MIN((published_at AT TIME ZONE 'UTC')::date) AS min_date,
              MAX((published_at AT TIME ZONE 'UTC')::date) AS max_date
            FROM social_posts_raw WHERE published_at IS NOT NULL;
            """,
            conn,
        )
        return platforms["platform"].tolist(), sources["source"].tolist(), langs["lang"].tolist(), date_bounds
    finally:
        conn.close()


@st.cache_data(ttl=300)
def fetch_keywords(date_from, date_to, platform, source, lang, top_k=30):
    conn = _conn()
    try:
        q = """
        SELECT date, platform, source, lang, keyword, score, n_docs
        FROM social_keywords_daily
        WHERE date >= %(date_from)s AND date <= %(date_to)s
          AND (%(platform)s = 'ALL' OR platform = %(platform)s)
          AND (%(source)s   = 'ALL' OR source   = %(source)s)
          AND (%(lang)s     = 'ALL' OR lang     = %(lang)s)
        ORDER BY date DESC, score DESC LIMIT %(limit)s;
        """
        return pd.read_sql(q, conn, params={
            "date_from": date_from, "date_to": date_to,
            "platform": platform, "source": source, "lang": lang, "limit": int(top_k),
        })
    finally:
        conn.close()


@st.cache_data(ttl=300)
def fetch_topics(date_from, date_to, platform, source, lang, top_k=25):
    conn = _conn()
    try:
        q = """
        SELECT date, platform, source, lang, topic_id, top_terms, weight, n_docs
        FROM social_topics_daily
        WHERE date >= %(date_from)s AND date <= %(date_to)s
          AND (%(platform)s = 'ALL' OR platform = %(platform)s)
          AND (%(source)s   = 'ALL' OR source   = %(source)s)
          AND (%(lang)s     = 'ALL' OR lang     = %(lang)s)
        ORDER BY date DESC, weight DESC LIMIT %(limit)s;
        """
        return pd.read_sql(q, conn, params={
            "date_from": date_from, "date_to": date_to,
            "platform": platform, "source": source, "lang": lang, "limit": int(top_k),
        })
    finally:
        conn.close()


@st.cache_data(ttl=300)
def fetch_keyword_trend(date_from, date_to, platform, source, lang, keyword):
    conn = _conn()
    try:
        q = """
        SELECT date, SUM(score) AS score
        FROM social_keywords_daily
        WHERE date >= %(date_from)s AND date <= %(date_to)s
          AND keyword = %(keyword)s
          AND (%(platform)s = 'ALL' OR platform = %(platform)s)
          AND (%(source)s   = 'ALL' OR source   = %(source)s)
          AND (%(lang)s     = 'ALL' OR lang     = %(lang)s)
        GROUP BY date ORDER BY date;
        """
        return pd.read_sql(q, conn, params={
            "date_from": date_from, "date_to": date_to,
            "platform": platform, "source": source, "lang": lang, "keyword": keyword,
        })
    finally:
        conn.close()


def render(filters: dict):
    section_header(
        "Social Media — Signaux publics",
        "Reddit · Mastodon · YouTube · TikTok — keywords & topics",
    )

    try:
        platforms, sources, langs, date_bounds = fetch_distinct_filters()
    except Exception as e:
        st.error(f"Impossible de se connecter à la base sociale : {e}")
        return

    soc_min = date_bounds.iloc[0]["min_date"]
    soc_max = date_bounds.iloc[0]["max_date"]

    if pd.isna(soc_min) or pd.isna(soc_max):
        st.warning("Aucune donnée sociale disponible. Lance d'abord l'ingestion Social.")
        return

    soc_min = pd.to_datetime(soc_min).date()
    soc_max = pd.to_datetime(soc_max).date()

    # Use global period clamped to social data range
    date_from = max(filters["start_date"], soc_min)
    date_to = min(filters["end_date"], soc_max)

    # Inline secondary filters
    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 1, 1])
    with c1:
        platform = st.selectbox("Plateforme", ["ALL"] + platforms, index=0, key="soc_plat")
    with c2:
        source = st.selectbox("Source", ["ALL"] + sources, index=0, key="soc_src")
    with c3:
        lang = st.selectbox("Langue", ["ALL"] + langs, index=0, key="soc_lang")
    with c4:
        top_k_kw = st.slider("Top keywords", 10, 80, 30, step=5, key="soc_kw")
    with c5:
        top_k_topics = st.slider("Top topics", 10, 80, 25, step=5, key="soc_tp")

    kw = fetch_keywords(date_from, date_to, platform, source, lang, top_k=top_k_kw)
    topics = fetch_topics(date_from, date_to, platform, source, lang, top_k=top_k_topics)

    c_left, c_right = st.columns(2)

    with c_left:
        st.subheader("Top keywords (TF-IDF)")
        if kw.empty:
            st.info("Aucun keyword. Lance d'abord extract_social_keywords.py")
        else:
            show = kw.copy()
            show["score"] = show["score"].round(6)
            st.dataframe(
                show[["date", "platform", "source", "lang", "keyword", "score", "n_docs"]],
                use_container_width=True, hide_index=True,
            )

    with c_right:
        st.subheader("Top topics (NMF)")
        if topics.empty:
            st.info("Aucun topic. Lance d'abord extract_social_topics.py")
        else:
            show = topics.copy()
            show["weight"] = show["weight"].round(6)
            show["top_terms_str"] = show["top_terms"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) else str(x)
            )
            st.dataframe(
                show[["date", "platform", "source", "lang", "topic_id", "weight", "n_docs", "top_terms_str"]],
                use_container_width=True, hide_index=True,
            )

    st.divider()
    st.subheader("Tendance d'un keyword")
    if kw.empty:
        st.info("Pas de keywords disponibles.")
        return

    keyword_choices = kw["keyword"].dropna().unique().tolist()
    if not keyword_choices:
        return

    pick_kw = st.selectbox("Keyword à tracer", keyword_choices, index=0, key="soc_trend_kw")
    trend = fetch_keyword_trend(date_from, date_to, platform, source, lang, pick_kw)

    if trend.empty:
        st.info("Pas de données de trend pour ce keyword.")
        return

    trend["date"] = pd.to_datetime(trend["date"])
    trend = trend.sort_values("date")
    st.line_chart(trend.set_index("date")["score"])

    with st.expander("Voir la table"):
        st.dataframe(trend, use_container_width=True, hide_index=True)
