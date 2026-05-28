# dashboard/views/analytics.py

import streamlit as st
import pandas as pd

from dashboard.data_access import get_connection
from dashboard.ui.components import section_header


def _load(query: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(query, conn, params=params)


def render(filters: dict):
    start_date = filters["start_date"]
    end_date = filters["end_date"]

    section_header(
        "Analytics Insights",
        "Analyses avancées : biais, spikes, durée de vie des mots-clés et sujets",
    )

    analysis = st.selectbox(
        "Analyse à explorer",
        ["Media Bias", "Topic Spikes", "Keyword Lifetime", "Topic Lifetime", "Theme Lifetime"],
    )

    if analysis == "Media Bias":
        st.subheader("Media Bias Scores")
        df = _load(
            "SELECT date, source, theme, bias_score, methodology "
            "FROM media_bias_scores ORDER BY date DESC, source, theme LIMIT 500"
        )
        if df.empty:
            st.info("Aucun score de biais disponible.")
            return

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]

        sources = sorted(df["source"].unique())
        selected_sources = st.multiselect("Sources", sources, default=sources)
        df = df[df["source"].isin(selected_sources)]

        st.dataframe(df, use_container_width=True)
        st.markdown("**Moyenne par thème et par média**")
        summary = (
            df.groupby(["source", "theme"])["bias_score"]
            .mean().reset_index()
            .sort_values("bias_score", ascending=False)
        )
        st.dataframe(summary, use_container_width=True)

    elif analysis == "Topic Spikes":
        st.subheader("Topic Spikes")
        df = _load(
            "SELECT date, topic_id, source, spike_score, baseline_window "
            "FROM spikes ORDER BY date DESC, spike_score DESC LIMIT 500"
        )
        if df.empty:
            st.info("Aucun spike détecté (pas assez d'historique).")
            return
        st.dataframe(df, use_container_width=True)
        st.markdown("**Top 20 spikes**")
        st.dataframe(df.sort_values("spike_score", ascending=False).head(20), use_container_width=True)

    elif analysis == "Keyword Lifetime":
        st.subheader("Keyword Lifetime")
        df = _load(
            "SELECT word, start_date, end_date, duration_days, total_frequency "
            "FROM keyword_lifetime ORDER BY start_date DESC LIMIT 500"
        )
        if df.empty:
            st.info("Aucune donnée de lifetime mots-clés.")
            return
        st.dataframe(df, use_container_width=True)
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Plus persistants**")
            st.dataframe(df.sort_values("duration_days", ascending=False).head(20), use_container_width=True)
        with col2:
            st.markdown("**Plus fréquents**")
            st.dataframe(df.sort_values("total_frequency", ascending=False).head(20), use_container_width=True)

    elif analysis == "Topic Lifetime":
        st.subheader("Topic Lifetime")
        df = _load(
            "SELECT topic_id, topic_label, first_seen_date, last_seen_date, peak_date, total_mentions "
            "FROM topic_lifetime ORDER BY first_seen_date DESC LIMIT 500"
        )
        if df.empty:
            st.info("Aucune donnée de lifetime topics.")
            return
        for col in ["first_seen_date", "last_seen_date", "peak_date"]:
            df[col] = pd.to_datetime(df[col])
        df = df.assign(duration_days=(df["last_seen_date"] - df["first_seen_date"]).dt.days + 1)
        st.dataframe(df, use_container_width=True)
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Plus persistants**")
            st.dataframe(df.sort_values("duration_days", ascending=False).head(20), use_container_width=True)
        with col2:
            st.markdown("**Plus couverts**")
            st.dataframe(df.sort_values("total_mentions", ascending=False).head(20), use_container_width=True)

    elif analysis == "Theme Lifetime":
        st.subheader("Theme Lifetime")
        df = _load(
            "SELECT theme, start_date, end_date, peak_date, total_mentions "
            "FROM theme_lifetime ORDER BY start_date DESC LIMIT 500"
        )
        if df.empty:
            st.info("Aucune donnée de lifetime thèmes.")
            return
        for col in ["start_date", "end_date", "peak_date"]:
            df[col] = pd.to_datetime(df[col])
        df = df.assign(duration_days=(df["end_date"] - df["start_date"]).dt.days + 1)
        st.dataframe(df, use_container_width=True)
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Plus persistants**")
            st.dataframe(df.sort_values("duration_days", ascending=False).head(20), use_container_width=True)
        with col2:
            st.markdown("**Plus couverts**")
            st.dataframe(df.sort_values("total_mentions", ascending=False).head(20), use_container_width=True)
