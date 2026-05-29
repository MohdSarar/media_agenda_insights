# dashboard/views/stance.py
# Feature 2 — Entity Stance Scoring

from __future__ import annotations
from io import StringIO
from datetime import date

import pandas as pd
import altair as alt
import streamlit as st

from dashboard.data_access import (
    load_entity_stance,
    load_entity_stance_trend,
    load_dashboard_config,
    count_articles_by_source,
)
from dashboard.ui.components import section_header, render_confidence

_LABEL_META = {
    "PER":  {"icon": "👤", "name": "Personnes"},
    "ORG":  {"icon": "🏛️", "name": "Organisations"},
    "LOC":  {"icon": "📍", "name": "Lieux"},
    "MISC": {"icon": "🔖", "name": "Divers"},
}


def _tone_color(score: float) -> str:
    if score > 0.05:
        return "#10b981"
    if score < -0.05:
        return "#ef4444"
    return "#64748b"


def render(filters: dict) -> None:
    start: date = filters["start_date"]
    end: date = filters["end_date"]

    section_header(
        "Stance des entités",
        "Comment chaque entité est-elle couverte — ton positif, négatif ou neutre ?",
    )

    st.markdown(
        """
        Le **score de stance** mesure si la couverture d'une entité (personne, organisation)
        est favorable ou critique. Il est calculé à partir de la co-occurrence de lemmes
        positifs/négatifs dans les articles mentionnant l'entité. Score ∈ [-1, 1] :
        **+1** = très favorable · **0** = neutre · **-1** = très critique.

        > **Prérequis** : lancer `processing/stance/score_entity_stance.py` pour peupler
        > la table `entity_stance_daily`.
        """
    )

    # ── Controls ──────────────────────────────────────────────────────────────
    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        label_choice = st.selectbox(
            "Type d'entités",
            options=["ALL"] + list(_LABEL_META.keys()),
            format_func=lambda k: "Tous types" if k == "ALL"
                else f"{_LABEL_META[k]['icon']} {_LABEL_META[k]['name']}",
            key="stance_label",
        )
    with c2:
        top_n = st.slider("Nombre d'entités", 10, 60, 30, 5, key="stance_top_n")
    with c3:
        min_n = load_dashboard_config().get("confidence", {}).get("min_n", 8)

    entity_label = None if label_choice == "ALL" else label_choice

    with st.spinner("Calcul des scores de stance…"):
        df = load_entity_stance(start, end, entity_label=entity_label, top_n=top_n)
        counts = count_articles_by_source(start, end)

    if df.empty:
        st.info(
            "Aucune donnée de stance pour cette période. "
            "Lancez d'abord : `python processing/stance/score_entity_stance.py "
            f"--start {start} --end {end}`"
        )
        return

    if counts:
        render_confidence(min(counts.values()), min_n)

    # ── KPIs ──────────────────────────────────────────────────────────────────
    agg_ent = (
        df.groupby("entity_text")
        .agg(
            mention_count=("mention_count", "sum"),
            net_score=("net_score", "mean"),
            positive_count=("positive_count", "sum"),
            negative_count=("negative_count", "sum"),
        )
        .reset_index()
        .sort_values("mention_count", ascending=False)
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Entités analysées", len(agg_ent))
    k2.metric("Mentions totales", f"{df['mention_count'].sum():,}")
    most_pos = agg_ent.sort_values("net_score", ascending=False).iloc[0]
    most_neg = agg_ent.sort_values("net_score").iloc[0]
    k3.metric("Plus favorable", most_pos["entity_text"][:22], f"{most_pos['net_score']:+.2f}")
    k4.metric("Plus critique", most_neg["entity_text"][:22], f"{most_neg['net_score']:+.2f}")

    st.markdown("---")

    # ── Section A: stance ranking bar chart ───────────────────────────────────
    st.markdown("#### Classement par score de stance (toutes sources)")

    chart_df = agg_ent.sort_values("net_score").head(top_n).copy()
    chart_df["color"] = chart_df["net_score"].apply(
        lambda s: "Positif" if s > 0.05 else ("Négatif" if s < -0.05 else "Neutre")
    )

    bar = (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
        .encode(
            x=alt.X("net_score:Q", title="Score de stance [-1 → +1]",
                    scale=alt.Scale(domain=[-1, 1])),
            y=alt.Y("entity_text:N", sort="x", title=None,
                    axis=alt.Axis(labelLimit=200, labelFontSize=11)),
            color=alt.Color(
                "color:N",
                scale=alt.Scale(
                    domain=["Positif", "Neutre", "Négatif"],
                    range=["#10b981", "#64748b", "#ef4444"],
                ),
                legend=alt.Legend(title="Ton"),
            ),
            tooltip=[
                alt.Tooltip("entity_text:N", title="Entité"),
                alt.Tooltip("net_score:Q", title="Score", format="+.3f"),
                alt.Tooltip("mention_count:Q", title="Mentions", format=",d"),
                alt.Tooltip("positive_count:Q", title="Mots positifs"),
                alt.Tooltip("negative_count:Q", title="Mots négatifs"),
            ],
        )
        .properties(height=max(300, len(chart_df) * 20 + 40))
        .configure_view(strokeWidth=0, fill="#0f172a")
        .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
        .configure_legend(labelColor="#94a3b8", titleColor="#94a3b8")
    )
    st.altair_chart(bar, use_container_width=True)

    # ── Section B: source comparison heatmap ──────────────────────────────────
    st.markdown("---")
    st.markdown("#### Stance par entité et par chaîne")

    pivot_df = df.copy()
    pivot_df["net_score"] = pivot_df["net_score"].fillna(0.0)
    pivot = pd.pivot_table(
        pivot_df,
        index="entity_text",
        columns="source",
        values="net_score",
        aggfunc="mean",
        fill_value=0.0,
    )
    pivot = pivot.loc[agg_ent.sort_values("mention_count", ascending=False)["entity_text"]]

    hm_long = pivot.reset_index().melt("entity_text", var_name="source", value_name="net_score")

    hm = (
        alt.Chart(hm_long)
        .mark_rect()
        .encode(
            x=alt.X("source:N", title="Chaîne", axis=alt.Axis(labelAngle=-30)),
            y=alt.Y("entity_text:N", title=None,
                    sort=agg_ent.sort_values("mention_count", ascending=False)["entity_text"].tolist()),
            color=alt.Color(
                "net_score:Q",
                scale=alt.Scale(scheme="redyellowgreen", domain=[-1, 1]),
                legend=alt.Legend(title="Score stance"),
            ),
            tooltip=[
                alt.Tooltip("entity_text:N", title="Entité"),
                alt.Tooltip("source:N", title="Chaîne"),
                alt.Tooltip("net_score:Q", title="Score", format="+.3f"),
            ],
        )
        .properties(height=max(250, min(len(pivot), 25) * 20 + 40))
        .configure_view(strokeWidth=0, fill="#0f172a")
        .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8")
        .configure_legend(labelColor="#94a3b8", titleColor="#94a3b8")
    )
    st.altair_chart(hm, use_container_width=True)

    # ── Section C: entity drill-down ──────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### Évolution temporelle d'une entité")

    all_entities = agg_ent["entity_text"].tolist()
    selected_entity = st.selectbox("Entité", all_entities, key="stance_entity_select")

    if selected_entity:
        trend_df = load_entity_stance_trend(selected_entity, start, end)

        if trend_df.empty:
            st.caption("Pas de données journalières pour cette entité.")
        else:
            line = (
                alt.Chart(trend_df)
                .mark_line(point=True, strokeWidth=2)
                .encode(
                    x=alt.X("date:T", title="Date", axis=alt.Axis(format="%d %b")),
                    y=alt.Y("net_score:Q", title="Score de stance",
                            scale=alt.Scale(domain=[-1, 1])),
                    color=alt.Color("source:N", title="Chaîne"),
                    tooltip=[
                        alt.Tooltip("date:T", format="%d %b %Y"),
                        "source",
                        alt.Tooltip("net_score:Q", title="Score", format="+.3f"),
                        alt.Tooltip("mention_count:Q", title="Articles", format=",d"),
                    ],
                )
                .properties(height=240, title=f"Évolution du score — {selected_entity}")
                .configure_view(strokeWidth=0, fill="#0f172a")
                .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
                .configure_legend(labelColor="#94a3b8", titleColor="#94a3b8")
            )
            st.altair_chart(line, use_container_width=True)

            # Per-source breakdown table
            src_agg = (
                trend_df.groupby("source")
                .agg(
                    articles=("mention_count", "sum"),
                    score_moyen=("net_score", "mean"),
                    positifs=("positive_count", "sum"),
                    négatifs=("negative_count", "sum"),
                )
                .reset_index()
                .sort_values("articles", ascending=False)
            )
            src_agg["score_moyen"] = src_agg["score_moyen"].round(3)
            st.dataframe(src_agg, use_container_width=True, hide_index=True)

    # ── Export ────────────────────────────────────────────────────────────────
    st.markdown("---")
    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    st.download_button(
        "⬇️ Exporter scores de stance (CSV)",
        data=csv_buf.getvalue(),
        file_name=f"stance_{start}_{end}.csv",
        mime="text/csv",
    )
