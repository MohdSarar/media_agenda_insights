# dashboard/views/compare.py

import streamlit as st
import pandas as pd
import altair as alt

from dashboard.data_access import load_topics_timeseries
from dashboard.ui.components import section_header


def render(filters: dict):
    start_date = filters["start_date"]
    end_date = filters["end_date"]

    section_header(
        "Comparaison entre chaînes",
        f"Couverture TV du {start_date} au {end_date}",
    )

    if start_date > end_date:
        st.warning("La date de début est supérieure à la date de fin.")
        return

    df_ts = load_topics_timeseries(start_date, end_date, media_type="tv")
    if df_ts.empty:
        st.info("Pas de données de sujets pour cette période.")
        return

    df_ts["source"] = df_ts["source"].fillna("Inconnu")

    # Heatmap
    st.subheader("Heatmap – Volume de sujets par chaîne et par jour")
    df_heat = df_ts.copy()
    df_heat["date"] = pd.to_datetime(df_heat["date"], errors="coerce")
    df_heat = df_heat.dropna(subset=["date"])
    df_heat["date_str"] = df_heat["date"].dt.strftime("%Y-%m-%d")

    heat_chart = (
        alt.Chart(df_heat)
        .mark_rect(cornerRadius=2)
        .encode(
            x=alt.X("date_str:N", title="Date", sort=sorted(df_heat["date_str"].unique())),
            y=alt.Y("source:N", title="Chaîne"),
            color=alt.Color(
                "total_articles:Q",
                title="Nb. articles",
                scale=alt.Scale(scheme="indigo"),
            ),
            tooltip=["date_str", "source", "total_articles"],
        )
        .properties(height=280)
    )
    st.altair_chart(heat_chart, use_container_width=True)

    # Line chart
    st.subheader("Évolution temporelle — Articles par chaîne")
    line_chart = (
        alt.Chart(df_ts)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("total_articles:Q", title="Articles"),
            color=alt.Color("source:N", title="Chaîne"),
            tooltip=["date", "source", "total_articles"],
        )
        .properties(height=300)
    )
    st.altair_chart(line_chart, use_container_width=True)
