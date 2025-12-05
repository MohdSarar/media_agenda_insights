# dashboard/views/compare.py

from datetime import date as date_type

import streamlit as st
import pandas as pd
import altair as alt

from data_access import (
    get_available_dates,
    load_topics_timeseries,
)


def render():
    st.title("📊 Comparaison entre chaînes")

    dates = get_available_dates()
    if not dates:
        st.error("Aucune donnée disponible.")
        return

    min_date, max_date = dates[0], dates[-1]

    with st.sidebar:
        st.header("Période d'analyse")
        start_date, end_date = st.date_input(
            "Intervalle de dates",
            value=(max_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        if isinstance(start_date, list) or isinstance(start_date, tuple):
            # streamlit peut renvoyer des tuples selon la version
            start_date, end_date = start_date

    if start_date > end_date:
        st.warning("La date de début est supérieure à la date de fin.")
        return

    st.markdown(
        f"Analyse de la période **{start_date} → {end_date}** (TV, tous sujets confondus)."
    )

    df_ts = load_topics_timeseries(start_date, end_date, media_type="tv")
    if df_ts.empty:
        st.info("Pas de données de sujets pour cette période.")
        return

    # Normalisation des noms de source
    df_ts["source"] = df_ts["source"].fillna("Inconnu")

    # --- Heatmap : intensité des sujets par date/source ---
    st.subheader("Heatmap – Volume de sujets par chaîne et par jour")

    df_heat = df_ts.copy()

    # ✅ forcer la colonne date en datetime
    df_heat["date"] = pd.to_datetime(df_heat["date"], errors="coerce")
    df_heat = df_heat.dropna(subset=["date"])

    df_heat["date_str"] = df_heat["date"].dt.strftime("%Y-%m-%d")


    heat_chart = (
        alt.Chart(df_heat)
        .mark_rect()
        .encode(
            x=alt.X("date_str:N", title="Date", sort=list(sorted(df_heat["date_str"].unique()))),
            y=alt.Y("source:N", title="Chaîne"),
            color=alt.Color("total_articles:Q", title="Nb. articles (tous topics)"),
            tooltip=["date_str", "source", "total_articles"],
        )
        .properties(height=300)
    )

    st.altair_chart(heat_chart, use_container_width=True)

    # --- Courbe : total articles par chaîne sur la période ---
    st.subheader("Évolution temporelle – Nombre total d'articles par chaîne")

    line_chart = (
        alt.Chart(df_ts)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("total_articles:Q", title="Articles (sujets agrégés)"),
            color=alt.Color("source:N", title="Chaîne"),
            tooltip=["date", "source", "total_articles"],
        )
        .properties(height=300)
    )

    st.altair_chart(line_chart, use_container_width=True)
