# dashboard/views/narratives.py

from datetime import date as date_type

import streamlit as st
import pandas as pd
import altair as alt
import plotly.graph_objects as go
import plotly.express as px

from dashboard.data_access import (
    get_available_dates,
    get_sources,
    load_keywords_range,
    load_lemmas_range,
    load_narrative_clusters,
    load_narrative_distribution_by_source,
)

try:
    from dashboard.ui.styles import CHART_COLORS, PLOTLY_TEMPLATE
except ImportError:
    CHART_COLORS = ["#6366f1", "#8b5cf6", "#ec4899", "#14b8a6", "#f59e0b", "#10b981", "#ef4444", "#3b82f6"]
    PLOTLY_TEMPLATE = {"layout": {}}

# Définition de grandes familles de narratifs (simplifiée)
THEME_DEFS = {
    "Sécurité / Police": [
        "sécurité", "insécurité", "police", "délinquance", "violence",
        "agression", "crime", "trafic", "prison"
    ],
    "Économie / Budget": [
        "budget", "inflation", "pouvoir", "achat", "économie", "impôt",
        "taxe", "chômage", "croissance"
    ],
    "Immigration / Identité": [
        "immigration", "migrant", "clandestin", "asile", "frontière",
        "identité", "banlieue"
    ],
    "Social / Santé / Éducation": [
        "école", "éducation", "professeur", "hôpital", "santé",
        "urgence", "médecin", "grève", "retraite"
    ],
    "International / Conflits": [
        "guerre", "ukraine", "russie", "israël", "gaza", "palestine",
        "otan", "conflit", "terrorisme"
    ],
}


def _compute_theme_bias(df_lemmas: pd.DataFrame, sources: list[str]) -> pd.DataFrame:
    """
    Calcule, pour chaque source et chaque thème, le nombre total de mentions
    des lemmes associés au thème.
    df_lemmas doit contenir : [source, media_type, lemma, total_count]
    """
    rows = []

    # On travaille en lower case pour être robuste
    df_lemmas = df_lemmas.copy()
    df_lemmas["lemma"] = df_lemmas["lemma"].astype(str).str.lower()

    for source in sources:
        df_src = df_lemmas[df_lemmas["source"] == source]

        for theme, words in THEME_DEFS.items():
            target_lemmas = [w.lower() for w in words]
            mask = df_src["lemma"].isin(target_lemmas)
            total = df_src.loc[mask, "total_count"].sum()
            rows.append(
                {"source": source, "theme": theme, "total_mentions": int(total)}
            )

    return pd.DataFrame(rows)



def _plot_bias_radar(df_theme: pd.DataFrame, selected_sources: list[str]):
    """
    Affiche un radar Plotly des thèmes par chaîne.
    """
    if df_theme.empty:
        st.info("Pas assez de données pour construire le radar.")
        return

    # Pivot: index = theme, colonnes = source
    pivot = df_theme.pivot(index="theme", columns="source", values="total_mentions").fillna(0)
    themes = pivot.index.tolist()

    fig = go.Figure()

    for source in selected_sources:
        if source not in pivot.columns:
            continue
        values = pivot[source].tolist()
        # fermer le polygone
        values_closed = values + [values[0]]
        theta_closed = themes + [themes[0]]

        fig.add_trace(
            go.Scatterpolar(
                r=values_closed,
                theta=theta_closed,
                fill="toself",
                name=source,
            )
        )

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True),
        ),
        showlegend=True,
        height=500,
    )

    st.plotly_chart(fig, use_container_width=True)


def render(filters: dict):
    st.title("🧩 Narratifs & biais médiatiques")
    start_date = filters["start_date"]
    end_date = filters["end_date"]
    media_type = filters["media_type"]
    global_source = filters["source"]

    dates = get_available_dates()
    if not dates:
        st.error("Aucune donnée disponible.")
        return

    min_date, max_date = dates[0], dates[-1]

    with st.sidebar:
        st.header("Période & chaînes")

        
        with st.expander("Options narratifs", expanded=False):
            all_sources = [s for s in get_sources() if s != "ALL"]
            selected_sources = st.multiselect(
                "Sources",
                options=all_sources,
                default=all_sources if global_source == "ALL" else [global_source],
            )
            top_n = st.slider("Top N mots par chaîne", 5, 30, 10)


        top_n = st.slider("Top N mots par chaîne", min_value=5, max_value=30, value=10, key="narratives_top_n")

    if not selected_sources:
        st.info("Sélectionnez au moins une chaîne.")
        return

    st.markdown(
        f"Analyse des narratifs sur la période **{start_date} → {end_date}** "
        f"pour les chaînes : {', '.join(selected_sources)}."
    )

        # Charger les mots-clés agrégés sur la période (pour l'affichage "Top mots")
    df_kw = load_keywords_range(start_date, end_date, media_type="tv")
    df_kw = df_kw[df_kw["source"].isin(selected_sources)]

    # Charger les lemmes agrégés sur la période (pour le radar & thèmes)
    df_lemmas = load_lemmas_range(start_date, end_date, media_type="tv")
    df_lemmas = df_lemmas[df_lemmas["source"].isin(selected_sources)]

    if df_kw.empty or df_lemmas.empty:
        st.info("Pas assez de données sur cette période / ces chaînes.")
        return


    # ───────────────────────── SECTION 1 : Top mots par chaîne ─────────────────────────
    st.subheader("🔎 Top mots-clés par chaîne")

    # Pour chaque source, garder les top_n mots
    df_top = (
        df_kw.sort_values(["source", "total_count"], ascending=[True, False])
        .groupby("source")
        .head(top_n)
    )

    chart = (
        alt.Chart(df_top)
        .mark_bar()
        .encode(
            x=alt.X("total_count:Q", title="Occurrences"),
            y=alt.Y("word:N", sort="-x", title="Mot"),
            color=alt.Color("source:N", title="Chaîne"),
            column=alt.Column("source:N", title=""),
            tooltip=["source", "word", "total_count"],
        )
        .properties(height=300)
    )

    st.altair_chart(chart, use_container_width=True)

    # ───────────────────────── SECTION 2 : Radar 'media bias' (basé sur les lemmes) ─────────────────────────
    st.subheader("🧭 Radar thématique – 'Media Bias' (basé sur les lemmes)")

    df_theme = _compute_theme_bias(df_lemmas, selected_sources)
    _plot_bias_radar(df_theme, selected_sources)

    # ───────────────────────── SECTION 3 : Tableau détaillé des thèmes ─────────────────────────
    st.subheader("📋 Détail des volumes par thème et par chaîne")

    if not df_theme.empty:
        pivot = df_theme.pivot(index="theme", columns="source", values="total_mentions").fillna(0)
        st.dataframe(pivot, use_container_width=True)
    else:
        st.info("Pas de données de thèmes sur cette période.")

        # ───────────────────────── SECTION 4 : Narratifs IA (clusters embeddings) ─────────────────────────
    st.subheader("🧠 Narratifs découverts par clustering sémantique")

    df_clusters = load_narrative_clusters()
    df_dist = load_narrative_distribution_by_source()

    if df_clusters.empty or df_dist.empty:
        st.info(
            "Aucun cluster de narratif trouvé. "
            "Lance d'abord le pipeline : `python processing/narratives/embed_and_cluster.py`."
        )
        return

    # Top narratifs
    st.markdown("**Top narratifs (tous médias confondus)**")
    st.dataframe(
        df_clusters[["cluster_id", "label", "top_keywords", "size"]],
        use_container_width=True,
    )

    # Distribution par chaîne (heatmap)
    st.markdown("**Présence des narratifs par chaîne (nombre d'articles)**")

    df_heat = df_dist.merge(
        df_clusters[["cluster_id", "label"]],
        on="cluster_id",
        how="left",
    )

    pivot = pd.pivot_table(
    df_heat,
    index="label",
    columns="source",
    values="article_count",
    aggfunc="sum",
    fill_value=0,
    )


    chart = (
        alt.Chart(pivot.reset_index().melt("label", var_name="source", value_name="articles"))
        .mark_rect()
        .encode(
            x=alt.X("source:N", title="Chaîne"),
            y=alt.Y("label:N", title="Narratif"),
            color=alt.Color("articles:Q", title="Nb d'articles"),
            tooltip=["label", "source", "articles"],
        )
        .properties(height=400)
    )

    st.altair_chart(chart, use_container_width=True)
