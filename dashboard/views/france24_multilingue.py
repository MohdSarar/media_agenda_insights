# dashboard/views/france24_multilingue.py

import pandas as pd
import streamlit as st
import altair as alt

from dashboard.data_access import get_connection


def load_df(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def chart_volume_by_lang_topics(df_topics: pd.DataFrame):
    # df_topics: date, lang, articles_count
    df = df_topics.copy()
    df["date"] = pd.to_datetime(df["date"])

    return (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("articles_count:Q", title="Volume (nb d'articles - topics)"),
            color=alt.Color("lang:N", title="Langue"),
            tooltip=["date:T", "lang:N", "articles_count:Q"],
        )
        .properties(height=280)
    )


def chart_volume_by_source(df_topics_sources: pd.DataFrame):
    # df_topics_sources: source, articles_count
    return (
        alt.Chart(df_topics_sources)
        .mark_bar()
        .encode(
            x=alt.X("articles_count:Q", title="Volume (nb d'articles - topics)"),
            y=alt.Y("source:N", sort="-x", title="Version France 24"),
            tooltip=["source:N", "articles_count:Q"],
        )
        .properties(height=220)
    )


def chart_top_topics_faceted(df_top_topics: pd.DataFrame):
    # df_top_topics: lang, topic_label, articles_count
    # On garde un top lisible
    return (
        alt.Chart(df_top_topics)
        .mark_bar()
        .encode(
            x=alt.X("articles_count:Q", title="Nb d'articles"),
            y=alt.Y("topic_label:N", sort="-x", title="Topic"),
            tooltip=["lang:N", "topic_label:N", "articles_count:Q"],
        )
        .facet(
            row=alt.Row("lang:N", title="Langue")
        )
        .properties(bounds="flush")
    )


def render():
    st.subheader("🌍 France 24 multilingue (FR / EN / ES / AR)")

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        days = st.selectbox("Fenêtre d'analyse", [3, 7, 14, 30], index=1)
    with col2:
        include_all = st.toggle("Inclure source='ALL'", value=False)
    with col3:
        show_tables = st.toggle("Afficher les tables", value=True)

    # ---- TOPICS: comparatifs multi-langues ----
    sql_volume_lang = """
        SELECT
            date,
            lang,
            SUM(articles_count) AS articles_count
        FROM topics_daily_f24
        WHERE date >= CURRENT_DATE - %(days)s
          AND (%(include_all)s = TRUE OR source <> 'ALL')
        GROUP BY date, lang
        ORDER BY date ASC;
    """
    df_vol_lang = load_df(sql_volume_lang, {"days": days, "include_all": include_all})

    sql_volume_source = """
        SELECT
            source,
            SUM(articles_count) AS articles_count
        FROM topics_daily_f24
        WHERE date >= CURRENT_DATE - %(days)s
          AND (%(include_all)s = TRUE OR source <> 'ALL')
        GROUP BY source
        ORDER BY articles_count DESC;
    """
    df_vol_source = load_df(sql_volume_source, {"days": days, "include_all": include_all})

    sql_top_topics = """
        WITH agg AS (
            SELECT
                lang,
                topic_label,
                SUM(articles_count) AS articles_count
            FROM topics_daily_f24
            WHERE date >= CURRENT_DATE - %(days)s
              AND (%(include_all)s = TRUE OR source <> 'ALL')
              AND topic_label IS NOT NULL
              AND topic_label <> ''
            GROUP BY lang, topic_label
        )
        SELECT *
        FROM (
            SELECT
                lang,
                topic_label,
                articles_count,
                ROW_NUMBER() OVER (PARTITION BY lang ORDER BY articles_count DESC) AS rn
            FROM agg
        ) t
        WHERE rn <= 8
        ORDER BY lang, articles_count DESC;
    """
    df_top_topics = load_df(sql_top_topics, {"days": days, "include_all": include_all})

    st.markdown("### 📊 Comparatifs éditoriaux (basés sur `topics_daily_f24`)")

    if df_vol_lang.empty:
        st.info("Pas assez de données topics sur la période.")
    else:
        st.altair_chart(chart_volume_by_lang_topics(df_vol_lang), use_container_width=True)

    if not df_vol_source.empty:
        st.markdown("**Répartition par version (source)**")
        st.altair_chart(chart_volume_by_source(df_vol_source), use_container_width=True)

    if not df_top_topics.empty:
        st.markdown("**Top topics par langue (Top 8)**")
        st.altair_chart(chart_top_topics_faceted(df_top_topics), use_container_width=True)

    # ---- TABLES détaillées (optionnel) ----
    if show_tables:
        st.markdown("### 🧠 Table topics (détails)")
        sql_topics_details = """
            SELECT date, source, lang, topic_id, topic_label, articles_count, keywords
            FROM topics_daily_f24
            WHERE date >= CURRENT_DATE - %(days)s
              AND (%(include_all)s = TRUE OR source <> 'ALL')
            ORDER BY date DESC, articles_count DESC
            LIMIT 200;
        """
        dft = load_df(sql_topics_details, {"days": days, "include_all": include_all})
        st.dataframe(dft, use_container_width=True)

        st.markdown("### 🔑 Table keywords (détails)")
        sql_keywords_details = """
            SELECT date, source, lang, word, count, rank
            FROM keywords_daily_f24
            WHERE date >= CURRENT_DATE - %(days)s
            ORDER BY date DESC, source, rank ASC
            LIMIT 200;
        """
        dfk = load_df(sql_keywords_details, {"days": days})
        st.dataframe(dfk, use_container_width=True)
