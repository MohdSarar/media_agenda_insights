import os
import datetime as dt
import pandas as pd
import streamlit as st
import psycopg2


DB_URL = os.getenv("DATABASE_URL")


def get_conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL introuvable (.env).")
    return psycopg2.connect(DB_URL)


@st.cache_data(ttl=600)  # 10min
def fetch_distinct_filters():
    conn = get_conn()
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
            FROM social_posts_raw
            WHERE published_at IS NOT NULL;
            """,
            conn,
        )
        return platforms["platform"].tolist(), sources["source"].tolist(), langs["lang"].tolist(), date_bounds
    finally:
        conn.close()


@st.cache_data(ttl=300)  # 5min
def fetch_keywords(date_from, date_to, platform, source, lang, top_k=30):
    conn = get_conn()
    try:
        q = """
        SELECT date, platform, source, lang, keyword, score, n_docs
        FROM social_keywords_daily
        WHERE date >= %(date_from)s AND date <= %(date_to)s
          AND (%(platform)s = 'ALL' OR platform = %(platform)s)
          AND (%(source)s   = 'ALL' OR source   = %(source)s)
          AND (%(lang)s     = 'ALL' OR lang     = %(lang)s)
        ORDER BY date DESC, score DESC
        LIMIT %(limit)s;
        """
        return pd.read_sql(
            q,
            conn,
            params={
                "date_from": date_from,
                "date_to": date_to,
                "platform": platform,
                "source": source,
                "lang": lang,
                "limit": int(top_k),
            },
        )
    finally:
        conn.close()


@st.cache_data(ttl=300)  # 5min
def fetch_topics(date_from, date_to, platform, source, lang, top_k=25):
    conn = get_conn()
    try:
        q = """
        SELECT date, platform, source, lang, topic_id, top_terms, weight, n_docs
        FROM social_topics_daily
        WHERE date >= %(date_from)s AND date <= %(date_to)s
          AND (%(platform)s = 'ALL' OR platform = %(platform)s)
          AND (%(source)s   = 'ALL' OR source   = %(source)s)
          AND (%(lang)s     = 'ALL' OR lang     = %(lang)s)
        ORDER BY date DESC, weight DESC
        LIMIT %(limit)s;
        """
        return pd.read_sql(
            q,
            conn,
            params={
                "date_from": date_from,
                "date_to": date_to,
                "platform": platform,
                "source": source,
                "lang": lang,
                "limit": int(top_k),
            },
        )
    finally:
        conn.close()


@st.cache_data(ttl=300)  # 5min
def fetch_keyword_trend(date_from, date_to, platform, source, lang, keyword):
    """
    Time series of one keyword across days (score).
    """
    conn = get_conn()
    try:
        q = """
        SELECT date, SUM(score) AS score
        FROM social_keywords_daily
        WHERE date >= %(date_from)s AND date <= %(date_to)s
          AND keyword = %(keyword)s
          AND (%(platform)s = 'ALL' OR platform = %(platform)s)
          AND (%(source)s   = 'ALL' OR source   = %(source)s)
          AND (%(lang)s     = 'ALL' OR lang     = %(lang)s)
        GROUP BY date
        ORDER BY date;
        """
        return pd.read_sql(
            q,
            conn,
            params={
                "date_from": date_from,
                "date_to": date_to,
                "platform": platform,
                "source": source,
                "lang": lang,
                "keyword": keyword,
            },
        )
    finally:
        conn.close()


def render():
    st.title("Social observable (Cercle 1)")
    st.caption("Signaux publics (Reddit/Mastodon/YouTube/TikTok) : keywords & topics, sans mélange avec TV/Presse.")

    # -------------------------
    # Filters
    # -------------------------
    platforms, sources, langs, date_bounds = fetch_distinct_filters()

    min_date = date_bounds.iloc[0]["min_date"]
    max_date = date_bounds.iloc[0]["max_date"]

    if pd.isna(min_date) or pd.isna(max_date):
        st.warning("Aucune donnée social_posts_raw (published_at manquant). Lance d’abord l’ingestion Social.")
        return

    min_date = pd.to_datetime(min_date).date()
    max_date = pd.to_datetime(max_date).date()

    with st.sidebar:
        st.header("Filtres")

        # Default window: last 3 days
        default_from = max(min_date, max_date - dt.timedelta(days=3))
        date_from, date_to = st.date_input(
            "Période",
            value=(default_from, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(date_from, tuple) or isinstance(date_from, list):
            # streamlit sometimes returns a tuple
            date_from, date_to = date_from

        platform = st.selectbox("Platform", ["ALL"] + platforms, index=0)
        source = st.selectbox("Source", ["ALL"] + sources, index=0)
        lang = st.selectbox("Lang", ["ALL"] + langs, index=0)

        top_k_kw = st.slider("Top keywords", 10, 80, 30, step=5)
        top_k_topics = st.slider("Top topics", 10, 80, 25, step=5)

    # -------------------------
    # Data
    # -------------------------
    kw = fetch_keywords(date_from, date_to, platform, source, lang, top_k=top_k_kw)
    topics = fetch_topics(date_from, date_to, platform, source, lang, top_k=top_k_topics)

    # -------------------------
    # Layout
    # -------------------------
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Top keywords (TF-IDF)")
        if kw.empty:
            st.info("Aucun keyword pour ces filtres. Lance d’abord extract_social_keywords.py")
        else:
            # show compact table
            show_kw = kw.copy()
            show_kw["score"] = show_kw["score"].round(6)
            st.dataframe(
                show_kw[["date", "platform", "source", "lang", "keyword", "score", "n_docs"]],
                use_container_width=True,
                hide_index=True,
            )

    with c2:
        st.subheader("Top topics (NMF)")
        if topics.empty:
            st.info("Aucun topic pour ces filtres. Lance d’abord extract_social_topics.py")
        else:
            show_topics = topics.copy()
            show_topics["weight"] = show_topics["weight"].round(6)

            # top_terms may arrive as list or string; normalize display
            def fmt_terms(x):
                if isinstance(x, list):
                    return ", ".join(x)
                if isinstance(x, str):
                    return x
                return ""

            show_topics["top_terms_str"] = show_topics["top_terms"].apply(fmt_terms)

            st.dataframe(
                show_topics[["date", "platform", "source", "lang", "topic_id", "weight", "n_docs", "top_terms_str"]],
                use_container_width=True,
                hide_index=True,
            )

    st.divider()

    # -------------------------
    # Trend explorer
    # -------------------------
    st.subheader("Keyword trend (exploration)")
    if kw.empty:
        st.info("Charge d’abord des keywords.")
        return

    keyword_choices = kw["keyword"].dropna().unique().tolist()
    if not keyword_choices:
        st.info("Aucun keyword exploitable.")
        return

    pick_kw = st.selectbox("Choisir un keyword", keyword_choices, index=0)
    trend = fetch_keyword_trend(date_from, date_to, platform, source, lang, pick_kw)

    if trend.empty:
        st.info("Pas de points de trend disponibles pour ce keyword.")
        return

    trend["date"] = pd.to_datetime(trend["date"])
    trend = trend.sort_values("date")
    st.line_chart(trend.set_index("date")["score"])

    with st.expander("Voir la table du trend"):
        st.dataframe(trend, use_container_width=True, hide_index=True)


# Streamlit entry point
if __name__ == "__main__":
    render()
