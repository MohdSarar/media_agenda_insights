# dashboard/views/france24_multilingue.py

import pandas as pd
import streamlit as st

from data_access import get_connection




def load_df(sql, params=None):
    conn = get_connection()
    return pd.read_sql(sql, conn, params=params)



def render():
    st.subheader("🌍 France 24 multilingue")

    lang = st.selectbox("Langue", ["fr", "en", "es", "ar"], index=0)

    # --- TOPICS (France 24 seulement) ---
    sql_topics = """
        SELECT date, source, lang, topic_id, topic_label, articles_count, keywords
        FROM topics_daily_f24
        WHERE lang = %(lang)s
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
        ORDER BY date DESC, rank ASC
        LIMIT 200;
    """
    dfk = load_df(sql_keywords, {"lang": lang})

    st.markdown("### 🔑 Mots-clés (keywords)")
    if dfk.empty:
        st.info("Aucun mot-clé disponible pour cette langue.")
    else:
        st.dataframe(dfk, use_container_width=True)

    # Optionnel : mini synthèse rapide
    st.markdown("### 📌 Lecture rapide")
    if not dft.empty:
        top_sources = dft["source"].value_counts().head(10)
        st.write("Sources les plus présentes (sur l’échantillon affiché) :")
        st.dataframe(top_sources, use_container_width=True)
