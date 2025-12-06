# dashboard/views/analytics.py

import streamlit as st
import pandas as pd
from datetime import date, timedelta

from data_access import get_connection


def load_df(query: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    # ❌ ne pas fermer la connexion ici, elle est gérée par data_access / Streamlit
    df = pd.read_sql(query, conn, params=params)
    return df


def render():
    st.title("📊 Analytics Insights")

    st.markdown(
        """
        Cette section présente les analyses avancées calculées à partir des données TV / presse :  

        - **Media Bias Scores** : répartition des thèmes par média  
        - **Topic Spikes** : emballements médiatiques (pics de couverture)  
        - **Keyword Lifetime** : durée de vie des mots-clés dans les médias  
        - **Topic Lifetime** : apparition, pic et disparition des sujets  
        - **Theme Lifetime** : persistance des grands thèmes narratifs  
        """
    )

    analysis = st.selectbox(
        "Choisissez une analyse à explorer :",
        [
            "Media Bias",
            "Topic Spikes",
            "Keyword Lifetime",
            "Topic Lifetime",
            "Theme Lifetime",
        ],
    )

    if analysis == "Media Bias":
        st.subheader("📌 Media Bias Scores (par thème et source)")

        query = """
            SELECT date, source, theme, bias_score, methodology
            FROM media_bias_scores
            ORDER BY date DESC, source, theme
            LIMIT 500
        """
        df = load_df(query)

        if df.empty:
            st.info("Aucun score de biais disponible pour l’instant.")
            return

        # 🔧 Normaliser la colonne date au format date (pas Timestamp)
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # Filtre source et date
        sources = sorted(df["source"].unique())
        selected_sources = st.multiselect("Filtrer les sources :", sources, default=sources)

        min_date = df["date"].min()
        max_date = df["date"].max()
        date_range = st.slider(
            "Période",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
        )

        # 🔧 Ici on compare date à date, sans pd.to_datetime
        mask = (
            df["source"].isin(selected_sources)
            & (df["date"] >= date_range[0])
            & (df["date"] <= date_range[1])
        )
        df_filtered = df[mask]

        st.dataframe(df_filtered)

        st.markdown("**Moyenne des biais par thème et par média :**")
        bias_summary = (
            df_filtered.groupby(["source", "theme"])["bias_score"]
            .mean()
            .reset_index()
            .sort_values("bias_score", ascending=False)
        )
        st.dataframe(bias_summary)


    elif analysis == "Topic Spikes":
        st.subheader("📌 Topic Spikes Detection")

        query = """
            SELECT date, topic_id, source, spike_score, baseline_window
            FROM spikes
            ORDER BY date DESC, spike_score DESC
            LIMIT 500
        """
        df = load_df(query)

        if df.empty:
            st.info("Aucun spike détecté pour l’instant (pas assez d’historique).")
            return

        st.dataframe(df)

        top_spikes = df.sort_values("spike_score", ascending=False).head(20)
        st.markdown("**Top spikes (score le plus élevé) :**")
        st.dataframe(top_spikes)

    elif analysis == "Keyword Lifetime":
        st.subheader("⏳ Keyword Lifetime")

        query = """
            SELECT word, start_date, end_date, duration_days, total_frequency
            FROM keyword_lifetime
            ORDER BY start_date DESC
            LIMIT 500
        """
        df = load_df(query)

        if df.empty:
            st.info("Aucune donnée de lifetime pour les mots-clés.")
            return

        st.dataframe(df)

        st.markdown("**Mots-clés les plus persistants (par durée) :**")
        longest = df.sort_values("duration_days", ascending=False).head(20)
        st.dataframe(longest)

        st.markdown("**Mots-clés les plus fréquents (par mentions totales) :**")
        most_freq = df.sort_values("total_frequency", ascending=False).head(20)
        st.dataframe(most_freq)

    elif analysis == "Topic Lifetime":
        st.subheader("📆 Topic Lifetime")

        query = """
            SELECT topic_id, topic_label, first_seen_date, last_seen_date,
                   peak_date, total_mentions
            FROM topic_lifetime
            ORDER BY first_seen_date DESC
            LIMIT 500
        """
        df = load_df(query)

        if df.empty:
            st.info("Aucune donnée de lifetime pour les topics.")
            return

        # 🔧 Normaliser les colonnes de dates au format datetime
        df["first_seen_date"] = pd.to_datetime(df["first_seen_date"])
        df["last_seen_date"] = pd.to_datetime(df["last_seen_date"])
        df["peak_date"] = pd.to_datetime(df["peak_date"])

        st.dataframe(df)

        st.markdown("**Topics les plus persistants :**")
        longest = (
            df.assign(
                duration_days=(df["last_seen_date"] - df["first_seen_date"]).dt.days + 1
            )
            .sort_values("duration_days", ascending=False)
            .head(20)
        )
        st.dataframe(longest)

        st.markdown("**Topics les plus couverts (mentions totales) :**")
        most_covered = df.sort_values("total_mentions", ascending=False).head(20)
        st.dataframe(most_covered)


    elif analysis == "Theme Lifetime":
        st.subheader("🎭 Theme Lifetime")

        query = """
            SELECT theme, start_date, end_date, peak_date, total_mentions
            FROM theme_lifetime
            ORDER BY start_date DESC
            LIMIT 500
        """
        df = load_df(query)

        if df.empty:
            st.info("Aucune donnée de lifetime pour les thèmes.")
            return

        # 🔧 Normaliser les colonnes de dates
        df["start_date"] = pd.to_datetime(df["start_date"])
        df["end_date"] = pd.to_datetime(df["end_date"])
        df["peak_date"] = pd.to_datetime(df["peak_date"])

        st.dataframe(df)

        st.markdown("**Thèmes les plus persistants :**")
        df = df.assign(
            duration_days=(df["end_date"] - df["start_date"]).dt.days + 1
        )
        longest = df.sort_values("duration_days", ascending=False).head(20)
        st.dataframe(longest)

        st.markdown("**Thèmes les plus couverts (mentions totales) :**")
        most_covered = df.sort_values("total_mentions", ascending=False).head(20)
        st.dataframe(most_covered)
