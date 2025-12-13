# dashboard/views/france24_multilingue.py

import pandas as pd
import streamlit as st
import altair as alt

from data_access import get_connection


def load_df(sql, params=None):
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        # évite les "connection already closed" / fuites
        try:
            conn.close()
        except Exception:
            pass


def topics_by_language(df):
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("articles_count:Q", title="Nombre d’articles"),
            y=alt.Y("topic_label:N", sort="-x", title="Sujet"),
            color=alt.Color("lang:N", title="Langue"),
            tooltip=["lang", "topic_label", "articles_count"]
        )
        .properties(height=400)
    )
    return chart


def language_distribution(df):
    # df = keywords -> colonne "count" (pas articles_count)
    agg = df.groupby("lang")["count"].sum().reset_index(name="total_count")

    chart = (
        alt.Chart(agg)
        .mark_bar()
        .encode(
            x=alt.X("lang:N", title="Langue"),
            y=alt.Y("total_count:Q", title="Volume (somme des occurrences mots-clés)"),
            color=alt.Color("lang:N", title="Langue"),
            tooltip=["lang", "total_count"]
        )
    )
    return chart


def render():
    st.subheader("🌍 France 24 multilingue")

    lang = st.selectbox("Langue", ["fr", "en", "es", "ar"], index=0)

    # --- TOPICS (France 24 seulement) ---
    sql_topics = """
        SELECT date, source, lang, topic_id, topic_label, articles_count, keywords
        FROM topics_daily_f24
        WHERE lang = %(lang)s
          AND source <> 'ALL'
        ORDER BY date DESC, articles_count DESC
        LIMIT 200;
    """
    dft = load_df(sql_topics, {"lang": lang})

    st.markdown("### 🧠 Top sujets (topics)")
    if dft.empty:
        st.info("Aucun topic disponible pour cette langue.")
    else:
        st.dataframe(dft, use_container_width=True)

    # --- KEYWORDS (France 24 seulement) ---
    sql_keywords = """
        SELECT date, source, lang, word, count, rank
        FROM keywords_daily_f24
        WHERE lang = %(lang)s
          AND source <> 'ALL'
        ORDER BY date DESC, rank ASC
        LIMIT 200;
    """
    dfk = load_df(sql_keywords, {"lang": lang})

    # ✅ Graphiques
    st.subheader("Comparatif (sur la langue sélectionnée)")
    if not dft.empty:
        st.altair_chart(topics_by_language(dft), use_container_width=True)

    if not dfk.empty:
        st.altair_chart(language_distribution(dfk), use_container_width=True)

    st.markdown("### 🔑 Mots-clés (keywords)")
    if dfk.empty:
        st.info("Aucun mot-clé disponible pour cette langue.")
    else:
        st.dataframe(dfk, use_container_width=True)

    st.markdown("### 📌 Lecture rapide")
    if not dft.empty:
        top_sources = dft["source"].value_counts().head(10)
        st.write("Sources les plus présentes (sur l’échantillon affiché) :")
        st.dataframe(top_sources, use_container_width=True)
