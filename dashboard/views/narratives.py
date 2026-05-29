# dashboard/views/narratives.py

import streamlit as st
import pandas as pd
import altair as alt
import plotly.graph_objects as go

from dashboard.data_access import (
    get_sources,
    load_keywords_range,
    load_lemmas_range,
    load_narrative_clusters,
    load_narrative_distribution_by_source,
    count_articles_by_source,
    load_dashboard_config,
)
from dashboard.ui.components import section_header, render_confidence

THEME_DEFS = {
    "Sécurité / Police": [
        "sécurité", "insécurité", "police", "délinquance", "violence",
        "agression", "crime", "trafic", "prison",
    ],
    "Économie / Budget": [
        "budget", "inflation", "pouvoir", "achat", "économie", "impôt",
        "taxe", "chômage", "croissance",
    ],
    "Immigration / Identité": [
        "immigration", "migrant", "clandestin", "asile", "frontière",
        "identité", "banlieue",
    ],
    "Social / Santé / Éducation": [
        "école", "éducation", "professeur", "hôpital", "santé",
        "urgence", "médecin", "grève", "retraite",
    ],
    "International / Conflits": [
        "guerre", "ukraine", "russie", "israël", "gaza", "palestine",
        "otan", "conflit", "terrorisme",
    ],
}


def _compute_theme_bias(df_lemmas: pd.DataFrame, sources: list[str]) -> pd.DataFrame:
    rows = []
    df_lemmas = df_lemmas.copy()
    df_lemmas["lemma"] = df_lemmas["lemma"].astype(str).str.lower()

    for source in sources:
        df_src = df_lemmas[df_lemmas["source"] == source]
        for theme, words in THEME_DEFS.items():
            total = df_src.loc[df_src["lemma"].isin([w.lower() for w in words]), "total_count"].sum()
            rows.append({"source": source, "theme": theme, "total_mentions": int(total)})

    return pd.DataFrame(rows)


def _plot_bias_radar(df_theme: pd.DataFrame, selected_sources: list[str]):
    if df_theme.empty:
        st.info("Pas assez de données pour le radar.")
        return

    pivot = df_theme.pivot(index="theme", columns="source", values="total_mentions").fillna(0)
    themes = pivot.index.tolist()

    fig = go.Figure()
    for source in selected_sources:
        if source not in pivot.columns:
            continue
        values = pivot[source].tolist()
        fig.add_trace(go.Scatterpolar(
            r=values + [values[0]],
            theta=themes + [themes[0]],
            fill="toself",
            name=source,
        ))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True)),
        showlegend=True,
        height=480,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e2e8f0"),
    )
    st.plotly_chart(fig, use_container_width=True)


def render(filters: dict):
    start_date = filters["start_date"]
    end_date = filters["end_date"]

    section_header(
        "Narratifs & Biais médiatiques",
        f"Analyse thématique du {start_date} au {end_date}",
    )

    # Controls inline
    all_sources = [s for s in get_sources() if s != "ALL"]
    c1, c2 = st.columns([3, 1])
    with c1:
        selected_sources = st.multiselect(
            "Chaînes à analyser",
            options=all_sources,
            default=all_sources,
            key="narr_sources",
        )
    with c2:
        top_n = st.slider("Top N mots", 5, 30, 10, key="narr_top_n")

    if not selected_sources:
        st.info("Sélectionnez au moins une chaîne.")
        return

    df_kw = load_keywords_range(start_date, end_date, media_type="tv")
    df_kw = df_kw[df_kw["source"].isin(selected_sources)]

    df_lemmas = load_lemmas_range(start_date, end_date, media_type="tv")
    df_lemmas = df_lemmas[df_lemmas["source"].isin(selected_sources)]

    counts = count_articles_by_source(start_date, end_date, media_type="tv")
    min_n = load_dashboard_config().get("confidence", {}).get("min_n", 8)
    if counts and selected_sources:
        active_counts = {s: counts.get(s, 0) for s in selected_sources if s in counts}
        if active_counts:
            render_confidence(min(active_counts.values()), min_n)

    if df_kw.empty or df_lemmas.empty:
        st.info("Pas assez de données sur cette période / ces chaînes.")
        return

    # ── Section 1: Top mots par chaîne ───────────────────────────────────────
    st.subheader("Top mots-clés par chaîne")
    df_top = (
        df_kw.sort_values(["source", "total_count"], ascending=[True, False])
        .groupby("source")
        .head(top_n)
    )
    chart = (
        alt.Chart(df_top)
        .mark_bar(cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
        .encode(
            x=alt.X("total_count:Q", title="Occurrences"),
            y=alt.Y("word:N", sort="-x", title=None),
            color=alt.Color("source:N", title="Chaîne", legend=None),
            column=alt.Column("source:N", title=""),
            tooltip=["source", "word", "total_count"],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)

    # ── Section 2: Radar thématique ───────────────────────────────────────────
    st.subheader("Radar thématique — Media Bias")
    df_theme = _compute_theme_bias(df_lemmas, selected_sources)
    _plot_bias_radar(df_theme, selected_sources)

    # ── Section 3: Tableau des thèmes ────────────────────────────────────────
    st.subheader("Volumes par thème et par chaîne")
    if not df_theme.empty:
        pivot = df_theme.pivot(index="theme", columns="source", values="total_mentions").fillna(0)
        st.dataframe(pivot, use_container_width=True)
    else:
        st.info("Pas de données de thèmes sur cette période.")

    # ── Section 4: Clusters sémantiques ──────────────────────────────────────
    st.subheader("Narratifs IA — clustering sémantique")
    df_clusters = load_narrative_clusters()
    df_dist = load_narrative_distribution_by_source()

    if df_clusters.empty or df_dist.empty:
        st.info(
            "Aucun cluster trouvé. "
            "Lance d'abord : `python processing/narratives/embed_and_cluster.py`"
        )
        return

    st.markdown("**Top narratifs (tous médias)**")
    st.dataframe(
        df_clusters[["cluster_id", "label", "top_keywords", "size"]],
        use_container_width=True,
    )

    st.markdown("**Présence des narratifs par chaîne**")
    df_heat = df_dist.merge(df_clusters[["cluster_id", "label"]], on="cluster_id", how="left")
    pivot = pd.pivot_table(
        df_heat, index="label", columns="source",
        values="article_count", aggfunc="sum", fill_value=0,
    )
    chart = (
        alt.Chart(pivot.reset_index().melt("label", var_name="source", value_name="articles"))
        .mark_rect(cornerRadius=2)
        .encode(
            x=alt.X("source:N", title="Chaîne"),
            y=alt.Y("label:N", title="Narratif"),
            color=alt.Color("articles:Q", title="Articles", scale=alt.Scale(scheme="blues")),
            tooltip=["label", "source", "articles"],
        )
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)
