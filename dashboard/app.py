# dashboard/app.py

import streamlit as st

# ⚠️ Importer les vues depuis le sous-dossier "views" (même dossier que app.py)
from views import overview, compare, topics


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
        - Prêt pour déploiement cloud / Docker
        """
    )

    tabs = st.tabs(
        [
            "📺 Vue d'ensemble",
            "📊 Comparaison chaînes",
            "🧠 Exploration des sujets",
        ]
    )

    with tabs[0]:
        overview.render()

    with tabs[1]:
        compare.render()

    with tabs[2]:
        topics.render()


if __name__ == "__main__":
    main()
