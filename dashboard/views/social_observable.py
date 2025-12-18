# dashboard/views/social_observable.py

from __future__ import annotations

from datetime import date as date_type

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.data_access import get_connection


def _read_sql(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn, params=params)


@st.cache_data(ttl=300)
def _fetch_distinct_filters() -> dict:
    platforms = _read_sql("SELECT DISTINCT platform FROM social_posts_raw WHERE platform IS NOT NULL ORDER BY platform;")
    sources = _read_sql("SELECT DISTINCT source FROM social_posts_raw WHERE source IS NOT NULL ORDER BY source;")
    langs = _read_sql("SELECT DISTINCT lang FROM social_posts_clean WHERE lang IS NOT NULL ORDER BY lang;")
    return {
        "platforms": platforms["platform"].tolist() if not platforms.empty else [],
        "sources": sources["source"].tolist() if not sources.empty else [],
        "langs": langs["lang"].tolist() if not langs.empty else [],
    }


def render(filters: dict):
    st.subheader("🗣️ Observatoire social")

    start_date: date_type = filters["start_date"]
    end_date: date_type = filters["end_date"]

    # Some deployments won't have social tables yet
    try:
        opts = _fetch_distinct_filters()
    except Exception as e:
        st.info("Les tables social_* ne semblent pas encore disponibles sur cette base.")
        with st.expander("Détails erreur (debug)", expanded=False):
            st.code(str(e))
        return

    with st.expander("Filtres (facultatif)", expanded=False):
        platform = st.selectbox("Plateforme", ["ALL"] + opts["platforms"], index=0)
        source = st.selectbox("Source", ["ALL"] + opts["sources"], index=0)
        lang = st.selectbox("Langue", ["ALL"] + opts["langs"], index=0)
        top_k = st.slider("Top K keywords", 10, 80, 30, step=5)

    # --- KPIs: posts count on period ---
    sql_posts = """
        SELECT
          (published_at AT TIME ZONE 'UTC')::date AS date,
          platform,
          source,
          COUNT(*) AS posts
        FROM social_posts_raw
        WHERE published_at IS NOT NULL
          AND (published_at AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
          AND (%s = 'ALL' OR platform = %s)
          AND (%s = 'ALL' OR source = %s)
        GROUP BY 1,2,3
        ORDER BY 1;
    """
    df_posts = _read_sql(sql_posts, [start_date, end_date, platform, platform, source, source])

    total_posts = int(df_posts["posts"].sum()) if not df_posts.empty else 0
    c1, c2, c3 = st.columns(3)
    c1.metric("Posts", f"{total_posts:,}")
    c2.metric("Plateformes", f"{df_posts['platform'].nunique() if not df_posts.empty else 0:,}")
    c3.metric("Sources", f"{df_posts['source'].nunique() if not df_posts.empty else 0:,}")

    st.divider()

    col1, col2 = st.columns([1.25, 1.0], gap="large")

    with col1:
        st.markdown("### 📈 Volume (posts / jour)")
        if df_posts.empty:
            st.info("Aucun post sur la période avec ces filtres.")
        else:
            ts = (
                df_posts.groupby("date", as_index=False)["posts"]
                .sum()
                .assign(date=lambda d: pd.to_datetime(d["date"], errors="coerce"))
                .dropna(subset=["date"])
            )
            chart = (
                alt.Chart(ts)
                .mark_area()
                .encode(
                    x=alt.X("date:T", title=None),
                    y=alt.Y("posts:Q", title="Posts"),
                    tooltip=["date:T", "posts:Q"],
                )
                .properties(height=320)
            )
            st.altair_chart(chart, width="stretch")

    with col2:
        st.markdown("### 🏷️ Top keywords (score)")
        sql_kw = """
            SELECT keyword, SUM(score) AS score
            FROM social_keywords_daily
            WHERE date BETWEEN %s AND %s
              AND (%s = 'ALL' OR platform = %s)
              AND (%s = 'ALL' OR source   = %s)
              AND (%s = 'ALL' OR lang     = %s)
            GROUP BY keyword
            ORDER BY score DESC
            LIMIT %s;
        """
        df_kw = _read_sql(sql_kw, [start_date, end_date, platform, platform, source, source, lang, lang, top_k])

        if df_kw.empty:
            st.info("Aucun keyword sur la période (social_keywords_daily).")
        else:
            bar = (
                alt.Chart(df_kw)
                .mark_bar()
                .encode(
                    y=alt.Y("keyword:N", sort="-x", title=None),
                    x=alt.X("score:Q", title="Score"),
                    tooltip=["keyword:N", "score:Q"],
                )
                .properties(height=320)
            )
            st.altair_chart(bar, width="stretch")

    with st.expander("Détails (tables)", expanded=False):
        st.markdown("**Posts agrégés (date × plateforme × source)**")
        st.dataframe(df_posts, width="stretch", hide_index=True)
        st.markdown("**Keywords (score)**")
        st.dataframe(df_kw, width="stretch", hide_index=True)
