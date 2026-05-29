# dashboard/views/lifecycle.py

from __future__ import annotations
from io import StringIO
from datetime import date

import pandas as pd
import altair as alt
import streamlit as st

from dashboard.data_access import load_lifecycle
from dashboard.ui.components import section_header


def render(filters: dict) -> None:
    start: date = filters["start_date"]
    end: date = filters["end_date"]

    section_header(
        "Story Lifecycle Tracker",
        "Cycle de vie des sujets médiatiques — apparition, pic et disparition",
    )

    # ── Controls ──────────────────────────────────────────────────────────────
    ctrl1, ctrl2 = st.columns([1, 2])
    with ctrl1:
        top_n = st.slider("Nombre de sujets", 10, 60, 30, 5)
    with ctrl2:
        sort_by = st.radio(
            "Trier par",
            ["Mentions totales", "Durée", "Date d'apparition"],
            horizontal=True,
        )

    with st.spinner("Chargement du cycle de vie des sujets…"):
        df = load_lifecycle(start, end, top_n=top_n)

    if df.empty:
        st.info(
            "Aucune donnée de cycle de vie pour cette période. "
            "Vérifiez les tables topic_lifetime ou topics_daily."
        )
        return

    # ── Sort ──────────────────────────────────────────────────────────────────
    sort_map = {
        "Mentions totales": ("total_mentions", False),
        "Durée": ("duration_days", False),
        "Date d'apparition": ("first_seen", True),
    }
    sort_col, asc = sort_map[sort_by]
    df = df.sort_values(sort_col, ascending=asc).reset_index(drop=True)

    # ── KPIs ──────────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sujets suivis", len(df))
    k2.metric("Durée médiane (jours)", int(df["duration_days"].median()))
    k3.metric("Durée max (jours)", int(df["duration_days"].max()))
    k4.metric("Mentions totales (tous sujets)", f"{df['total_mentions'].sum():,}")

    st.markdown("---")

    # ── Gantt chart ───────────────────────────────────────────────────────────
    # Ensure datetime columns
    for col in ["first_seen", "last_seen", "peak_date"]:
        df[col] = pd.to_datetime(df[col])

    # Shorten labels that are too long
    df["short_label"] = df["topic_label"].str[:45]

    # Normalise mentions for color intensity
    max_mentions = df["total_mentions"].max() or 1
    df["intensity"] = df["total_mentions"] / max_mentions

    gantt_bars = (
        alt.Chart(df)
        .mark_bar(height=14, cornerRadiusEnd=4, cornerRadiusStart=4)
        .encode(
            x=alt.X("first_seen:T", title="Date", axis=alt.Axis(format="%d %b", labelColor="#94a3b8", titleColor="#94a3b8")),
            x2=alt.X2("last_seen:T"),
            y=alt.Y(
                "short_label:N",
                sort=None,
                title=None,
                axis=alt.Axis(labelColor="#cbd5e1", labelFontSize=11, labelLimit=300),
            ),
            color=alt.Color(
                "intensity:Q",
                scale=alt.Scale(scheme="bluepurple", domain=[0, 1]),
                legend=alt.Legend(title="Intensité (mentions)", orient="bottom"),
            ),
            tooltip=[
                alt.Tooltip("topic_label:N", title="Sujet"),
                alt.Tooltip("first_seen:T", title="Première apparition", format="%d %b %Y"),
                alt.Tooltip("last_seen:T", title="Dernière apparition", format="%d %b %Y"),
                alt.Tooltip("peak_date:T", title="Pic de couverture", format="%d %b %Y"),
                alt.Tooltip("total_mentions:Q", title="Mentions totales", format=",d"),
                alt.Tooltip("duration_days:Q", title="Durée (jours)"),
            ],
        )
    )

    # Peak markers
    peak_df = df.dropna(subset=["peak_date"])
    peak_marks = (
        alt.Chart(peak_df)
        .mark_point(shape="triangle-up", size=80, color="#f59e0b", filled=True, opacity=0.9)
        .encode(
            x=alt.X("peak_date:T"),
            y=alt.Y("short_label:N", sort=None),
            tooltip=[
                alt.Tooltip("topic_label:N", title="Sujet"),
                alt.Tooltip("peak_date:T", title="Pic", format="%d %b %Y"),
                alt.Tooltip("total_mentions:Q", title="Mentions au pic", format=",d"),
            ],
        )
    )

    chart_height = max(350, len(df) * 24 + 60)

    gantt = (
        (gantt_bars + peak_marks)
        .properties(height=chart_height, title="")
        .configure_view(strokeWidth=0, fill="#0f172a")
        .configure_axis(domainColor="#334155", gridColor="#1e293b")
    )

    st.altair_chart(gantt, use_container_width=True)

    st.caption("▲ Le triangle orange indique le pic de couverture du sujet.")

    # ── Distribution: duration histogram ──────────────────────────────────────
    st.markdown("#### Distribution des durées")
    hist_col, scatter_col = st.columns(2)

    with hist_col:
        hist = (
            alt.Chart(df)
            .mark_bar(color="#6366f1", opacity=0.85)
            .encode(
                x=alt.X("duration_days:Q", bin=alt.Bin(maxbins=20), title="Durée (jours)"),
                y=alt.Y("count():Q", title="Nombre de sujets"),
                tooltip=[
                    alt.Tooltip("duration_days:Q", bin=True, title="Durée"),
                    alt.Tooltip("count():Q", title="Sujets"),
                ],
            )
            .properties(height=220, title="Durées de vie")
            .configure_view(strokeWidth=0, fill="#0f172a")
            .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
        )
        st.altair_chart(hist, use_container_width=True)

    with scatter_col:
        sc = (
            alt.Chart(df)
            .mark_circle(color="#8b5cf6", opacity=0.8, size=80)
            .encode(
                x=alt.X("duration_days:Q", title="Durée (jours)"),
                y=alt.Y("total_mentions:Q", title="Mentions totales"),
                tooltip=[
                    alt.Tooltip("topic_label:N", title="Sujet"),
                    alt.Tooltip("duration_days:Q", title="Durée (jours)"),
                    alt.Tooltip("total_mentions:Q", title="Mentions", format=",d"),
                ],
            )
            .properties(height=220, title="Durée vs Mentions")
            .configure_view(strokeWidth=0, fill="#0f172a")
            .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
        )
        st.altair_chart(sc, use_container_width=True)

    # ── Data table ────────────────────────────────────────────────────────────
    with st.expander("Tableau détaillé", expanded=False):
        display_df = df[
            ["topic_label", "first_seen", "last_seen", "peak_date", "duration_days", "total_mentions"]
        ].copy()
        for col in ["first_seen", "last_seen", "peak_date"]:
            display_df[col] = display_df[col].dt.strftime("%d %b %Y")
        display_df.columns = ["Sujet", "1re apparition", "Dernière", "Pic", "Durée (j)", "Mentions"]
        display_df.index = range(1, len(display_df) + 1)
        st.dataframe(display_df, use_container_width=True)

    # ── CSV Export ────────────────────────────────────────────────────────────
    st.markdown("---")
    csv_buf = StringIO()
    df[
        ["topic_label", "first_seen", "last_seen", "peak_date", "duration_days", "total_mentions"]
    ].to_csv(csv_buf, index=False)
    st.download_button(
        label="⬇️ Exporter le cycle de vie (CSV)",
        data=csv_buf.getvalue(),
        file_name=f"story_lifecycle_{start}_{end}.csv",
        mime="text/csv",
    )
