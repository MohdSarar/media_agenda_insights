# dashboard/views/narratives.py

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.data_access import (
    get_sources,
    load_keywords_range,
    load_narrative_clusters,
    load_narrative_distribution_by_source,
)


def render(filters: dict):
    st.subheader("🧩 Narratives")

    start_date = filters["start_date"]
    end_date = filters["end_date"]
    media_type = filters.get("media_type") or "tv"
    global_source = filters.get("source", "ALL")

    with st.expander("Options (facultatif)", expanded=False):
        top_n = st.slider("Top N mots (par source)", 5, 30, 12, step=1)
        all_sources = [s for s in get_sources(media_type=None) if s != "ALL"]
        default_sources = all_sources if global_source == "ALL" else [global_source]
        selected_sources = st.multiselect("Sources", options=all_sources, default=default_sources)

    # --- Section 1: narrative clusters (global) ---
    st.markdown("### 🧠 Clusters de narratifs")
    df_clusters = load_narrative_clusters()
    if df_clusters.empty:
        st.info("Aucun cluster disponible (narratives_clusters).")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Clusters", f"{df_clusters['cluster_id'].nunique():,}")
        c2.metric("Taille max", f"{int(df_clusters['size'].max()):,}")
        c3.metric("Taille totale", f"{int(df_clusters['size'].sum()):,}")

        st.dataframe(
            df_clusters[["cluster_id", "label", "size", "top_keywords", "created_at"]],
            width="stretch",
            hide_index=True,
        )

    st.divider()

    # --- Section 2: distribution heatmap by source ---
    st.markdown("### 🗺️ Distribution des narratifs par source")
    df_dist = load_narrative_distribution_by_source()
    if df_dist.empty:
        st.info("Aucune distribution disponible (narratives_assignments).")
    else:
        if selected_sources:
            df_dist = df_dist[df_dist["source"].isin(selected_sources)]

        pivot = df_dist.pivot_table(index="cluster_id", columns="source", values="article_count", aggfunc="sum").fillna(0)
        pivot = pivot.sort_values(pivot.columns.tolist(), ascending=False) if not pivot.empty else pivot
        heat_df = pivot.reset_index().melt("cluster_id", var_name="source", value_name="articles")

        chart = (
            alt.Chart(heat_df)
            .mark_rect()
            .encode(
                x=alt.X("source:N", title=None),
                y=alt.Y("cluster_id:N", title="Cluster"),
                color=alt.Color("articles:Q", title="Articles"),
                tooltip=["cluster_id:N", "source:N", "articles:Q"],
            )
            .properties(height=420)
        )
        st.altair_chart(chart, width="stretch")

    st.divider()

    # --- Section 3: narrative signals (top keywords over period) ---
    st.markdown("### 🔤 Signaux narratifs (mots-clés sur la période)")
    df_kw = load_keywords_range(start_date, end_date, media_type=media_type)
    if df_kw.empty:
        st.info("Pas de mots-clés sur cette période (keywords_daily).")
        return

    if selected_sources:
        df_kw = df_kw[df_kw["source"].isin(selected_sources)]

    agg = (
        df_kw.groupby(["source", "word"], as_index=False)["total_count"]
        .sum()
        .sort_values(["source", "total_count"], ascending=[True, False])
    )
    agg["rank"] = agg.groupby("source")["total_count"].rank(method="first", ascending=False)
    top = agg[agg["rank"] <= top_n].copy()

    bar = (
        alt.Chart(top)
        .mark_bar()
        .encode(
            y=alt.Y("word:N", sort="-x", title=None),
            x=alt.X("total_count:Q", title="Mentions (somme)"),
            color=alt.Color("source:N", legend=alt.Legend(title="Source")),
            tooltip=["source:N", "word:N", "total_count:Q"],
        )
        .properties(height=420)
        .facet(row=alt.Row("source:N", header=alt.Header(title=None)))
    )
    st.altair_chart(bar, width="stretch")

    with st.expander("Détails (table)", expanded=False):
        st.dataframe(top.sort_values(["source", "total_count"], ascending=[True, False]), width="stretch", hide_index=True)
