# dashboard/views/compare.py

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.data_access import load_topics_timeseries


def render(filters: dict):
    st.subheader("📊 Comparaison – évolution par source")

    start_date = filters["start_date"]
    end_date = filters["end_date"]
    media_type = filters.get("media_type") or "tv"  # topics_daily is most meaningful for TV in your pipeline
    source = filters.get("source", "ALL")

    df_ts = load_topics_timeseries(start_date, end_date, media_type=media_type)
    if df_ts.empty:
        st.info("Aucune donnée sur cette période (topics_daily).")
        return

    if source != "ALL":
        df_ts = df_ts[df_ts["source"] == source]

    # Ensure date dtype
    df_ts["date"] = pd.to_datetime(df_ts["date"], errors="coerce")
    df_ts = df_ts.dropna(subset=["date"])

    # --- KPIs ---
    total = int(df_ts["total_articles"].sum()) if not df_ts.empty else 0
    n_sources = df_ts["source"].nunique()
    c1, c2, c3 = st.columns(3)
    c1.metric("Articles agrégés", f"{total:,}")
    c2.metric("Sources", f"{n_sources:,}")
    c3.metric("Media type", media_type)

    st.divider()

    col1, col2 = st.columns([1.2, 1.0], gap="large")

    with col1:
        st.markdown("### 📈 Courbe (articles / jour)")
        line_chart = (
            alt.Chart(df_ts)
            .mark_line(point=False)
            .encode(
                x=alt.X("date:T", title=None),
                y=alt.Y("total_articles:Q", title="Articles"),
                color=alt.Color("source:N", legend=alt.Legend(title="Source")),
                tooltip=["date:T", "source:N", "total_articles:Q"],
            )
            .properties(height=340)
        )
        st.altair_chart(line_chart, width="stretch")

    with col2:
        st.markdown("### 🧩 Part d'attention (stacked)")
        area = (
            alt.Chart(df_ts)
            .mark_area()
            .encode(
                x=alt.X("date:T", title=None),
                y=alt.Y("total_articles:Q", stack="normalize", title="Part"),
                color=alt.Color("source:N", legend=None),
                tooltip=["date:T", "source:N", "total_articles:Q"],
            )
            .properties(height=340)
        )
        st.altair_chart(area, width="stretch")

    with st.expander("Détails (table)", expanded=False):
        st.dataframe(df_ts.sort_values(["date", "source"]), width="stretch", hide_index=True)
