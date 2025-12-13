# dashboard/app.py

import streamlit as st

# ⚠️ Importer les vues depuis le sous-dossier "views" (même dossier que app.py)
from views import overview, compare, topics, narratives, analytics, france24_multilingue




def main():
    st.set_page_config(
        page_title="Media Agenda Insights",
        page_icon="🛰️",
        layout="wide",
    )

    st.sidebar.title("🛰️ Media Agenda Insights")
    st.sidebar.markdown(
        """
        **Projet Data Engineering / NLP**

        - Ingestion automatique des flux TV
        - NLP & lemmatisation (Stanza + spaCy)
        - Top mots-clés & sujets (topic modeling)
        - Comparaison multi-chaînes
        - Analyse de narratifs & 'media bias'
        - Architecture prête pour le cloud / Docker / big data
        """
    )

    tabs = st.tabs(
        [
            "📺 Vue d'ensemble",
            "📊 Comparaison chaînes",
            "🧠 Exploration des sujets",
            "🧩 Narratifs & biais médiatiques",
            "📊 Analytics Insights",
            "🌍 France 24 multilingue",
        ]
    )

    with tabs[0]:
        overview.render()

    with tabs[1]:
        compare.render()

    with tabs[2]:
        topics.render()

    with tabs[3]:
        narratives.render()

    with tabs[4]:
        analytics.render()
    with tabs[5]:
        france24_multilingue.render()


if __name__ == "__main__":
    main()
    