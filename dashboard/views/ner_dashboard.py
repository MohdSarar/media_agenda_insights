# dashboard/views/ner_dashboard.py

from __future__ import annotations
from io import StringIO
from datetime import date

import pandas as pd
import altair as alt
import streamlit as st

from dashboard.data_access import (
    load_ner_entities,
    load_entity_trend,
    load_entity_source_heatmap,
)
from dashboard.ui.components import section_header


_LABEL_META = {
    "PER":  {"icon": "👤", "name": "Personnes",       "color": "#6366f1"},
    "ORG":  {"icon": "🏛️", "name": "Organisations",   "color": "#10b981"},
    "LOC":  {"icon": "📍", "name": "Lieux",            "color": "#f59e0b"},
    "MISC": {"icon": "🔖", "name": "Divers",           "color": "#64748b"},
}


def render(filters: dict) -> None:
    start: date = filters["start_date"]
    end: date = filters["end_date"]

    section_header(
        "NER — Entités Nommées",
        "Personnes, organisations et lieux les plus mentionnés dans les médias",
    )

    # ── Controls ──────────────────────────────────────────────────────────────
    ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 2])
    with ctrl1:
        media_type = st.selectbox(
            "Type de media", ["tv", "press", "ALL"], key="ner_media_type"
        )
    with ctrl2:
        selected_labels = st.multiselect(
            "Types d'entités",
            options=list(_LABEL_META.keys()),
            default=["PER", "ORG", "LOC"],
            format_func=lambda k: f"{_LABEL_META[k]['icon']} {_LABEL_META[k]['name']}",
            key="ner_labels",
        )
    with ctrl3:
        top_n = st.slider("Nombre d'entités", 20, 200, 50, 10, key="ner_top_n")

    mt = None if media_type == "ALL" else media_type

    with st.spinner("Chargement des entités nommées…"):
        df = load_ner_entities(start, end, mt or "tv", selected_labels or None, top_n)

    if df.empty:
        st.info(
            "Aucune entité nommée pour cette période. "
            "Vérifiez que le pipeline NLP (process_articles.py) a bien tourné "
            "et que la colonne `entities` de `articles_clean` est peuplée."
        )
        return

    # ── Global KPIs ───────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Entités uniques", df["entity_text"].nunique())
    k2.metric("Mentions totales", f"{df['mention_count'].sum():,}")
    top_entity = df.iloc[0]["entity_text"] if not df.empty else "—"
    k3.metric("Entité #1", top_entity[:24])
    n_sources = df["source"].nunique() if "source" in df.columns else 0
    k4.metric("Sources couvertes", n_sources)

    st.markdown("---")

    # ── Per-label top bars ─────────────────────────────────────────────────────
    tab_labels = [
        f"{_LABEL_META[l]['icon']} {_LABEL_META[l]['name']}"
        for l in selected_labels
    ] + ["🔍 Recherche entité", "🗺️ Heatmap sources"]

    tabs = st.tabs(tab_labels)

    for i, label in enumerate(selected_labels):
        with tabs[i]:
            _render_label_tab(df, label, start, end, mt or "tv")

    with tabs[-2]:
        _render_search_tab(df, start, end, mt or "tv")
    with tabs[-1]:
        _render_heatmap_tab(start, end, mt or "tv")

    # ── CSV export ────────────────────────────────────────────────────────────
    st.markdown("---")
    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    st.download_button(
        "⬇️ Exporter entités (CSV)",
        data=csv_buf.getvalue(),
        file_name=f"ner_{start}_{end}.csv",
        mime="text/csv",
    )


def _render_label_tab(
    df: pd.DataFrame,
    label: str,
    start: date,
    end: date,
    media_type: str,
) -> None:
    meta = _LABEL_META[label]
    sub = df[df["entity_label"] == label].copy()

    if sub.empty:
        st.info(f"Aucune entité de type {meta['name']} trouvée.")
        return

    sub_agg = (
        sub.groupby("entity_text")["mention_count"]
        .sum()
        .reset_index()
        .sort_values("mention_count", ascending=False)
        .head(30)
    )

    col_chart, col_table = st.columns([3, 2])

    with col_chart:
        bar = (
            alt.Chart(sub_agg)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color=meta["color"])
            .encode(
                x=alt.X("mention_count:Q", title="Mentions"),
                y=alt.Y("entity_text:N", sort="-x", title=None,
                        axis=alt.Axis(labelLimit=200, labelFontSize=11)),
                tooltip=[
                    alt.Tooltip("entity_text:N", title=meta["name"]),
                    alt.Tooltip("mention_count:Q", title="Mentions", format=",d"),
                ],
            )
            .properties(height=min(600, len(sub_agg) * 22 + 40), title=f"Top {meta['name']}")
            .configure_view(strokeWidth=0, fill="#0f172a")
            .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
        )
        st.altair_chart(bar, use_container_width=True)

    with col_table:
        st.markdown(f"**Top {meta['name']} par source**")
        pivot = (
            sub.groupby(["entity_text", "source"])["mention_count"]
            .sum()
            .unstack(fill_value=0)
        )
        pivot["TOTAL"] = pivot.sum(axis=1)
        pivot = pivot.sort_values("TOTAL", ascending=False).head(20)
        st.dataframe(pivot, use_container_width=True)

    # Cross-source bar for top entity
    top_ent = sub_agg.iloc[0]["entity_text"] if not sub_agg.empty else None
    if top_ent:
        st.markdown(f"**Tendance journalière : `{top_ent}`**")
        _render_entity_trend(top_ent, start, end, media_type)


def _render_search_tab(df: pd.DataFrame, start: date, end: date, media_type: str) -> None:
    st.markdown("#### Rechercher une entité et voir sa tendance")

    # Autocomplete from loaded entities
    all_entities = sorted(df["entity_text"].unique().tolist())
    search_val = st.selectbox(
        "Entité à analyser",
        options=all_entities,
        key="ner_search_entity",
    )

    if search_val:
        info_row = df[df["entity_text"] == search_val]
        if not info_row.empty:
            r = info_row.iloc[0]
            label = r["entity_label"]
            meta = _LABEL_META.get(label, {"icon": "?", "name": label, "color": "#6366f1"})
            st.markdown(
                f"**Type :** {meta['icon']} {meta['name']}  |  "
                f"**Mentions totales :** {info_row['mention_count'].sum():,}"
            )

        _render_entity_trend(search_val, start, end, media_type)

        # Per-source breakdown
        src_df = (
            df[df["entity_text"] == search_val]
            .groupby("source")["mention_count"]
            .sum()
            .reset_index()
            .sort_values("mention_count", ascending=False)
        )
        if not src_df.empty:
            bar = (
                alt.Chart(src_df)
                .mark_bar(color="#6366f1", cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                .encode(
                    x=alt.X("mention_count:Q", title="Mentions"),
                    y=alt.Y("source:N", sort="-x", title="Chaîne"),
                    tooltip=["source", alt.Tooltip("mention_count:Q", format=",d")],
                )
                .properties(height=180, title="Répartition par chaîne")
                .configure_view(strokeWidth=0, fill="#0f172a")
                .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
            )
            st.altair_chart(bar, use_container_width=True)


def _render_entity_trend(entity: str, start: date, end: date, media_type: str) -> None:
    trend_df = load_entity_trend(entity, start, end, media_type)
    if trend_df.empty:
        st.caption("Pas de données de tendance pour cette entité.")
        return

    line = (
        alt.Chart(trend_df)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X("date:T", title="Date", axis=alt.Axis(format="%d %b")),
            y=alt.Y("mention_count:Q", title="Mentions"),
            color=alt.Color("source:N", title="Chaîne"),
            tooltip=[
                alt.Tooltip("date:T", format="%d %b %Y"),
                "source",
                alt.Tooltip("mention_count:Q", title="Mentions", format=",d"),
            ],
        )
        .properties(height=220)
        .configure_view(strokeWidth=0, fill="#0f172a")
        .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
        .configure_legend(labelColor="#94a3b8", titleColor="#94a3b8")
    )
    st.altair_chart(line, use_container_width=True)


def _render_heatmap_tab(start: date, end: date, media_type: str) -> None:
    st.markdown("#### Heatmap entités × sources")

    ctrl_col1, ctrl_col2 = st.columns(2)
    with ctrl_col1:
        hl = st.selectbox(
            "Type d'entité",
            options=list(_LABEL_META.keys()),
            format_func=lambda k: f"{_LABEL_META[k]['icon']} {_LABEL_META[k]['name']}",
            key="ner_heatmap_label",
        )
    with ctrl_col2:
        hn = st.slider("Top N entités", 5, 30, 15, 5, key="ner_heatmap_n")

    hm_df = load_entity_source_heatmap(start, end, media_type, hl, hn)

    if hm_df.empty:
        st.info("Pas de données pour cette combinaison.")
        return

    heatmap = (
        alt.Chart(hm_df)
        .mark_rect()
        .encode(
            x=alt.X("source:N", title="Chaîne", axis=alt.Axis(labelAngle=-30)),
            y=alt.Y("entity_text:N", title=None,
                    sort=alt.EncodingSortField("mention_count", op="sum", order="descending")),
            color=alt.Color(
                "mention_count:Q",
                scale=alt.Scale(scheme="purpleblue"),
                legend=alt.Legend(title="Mentions"),
            ),
            tooltip=[
                alt.Tooltip("entity_text:N", title="Entité"),
                alt.Tooltip("source:N", title="Chaîne"),
                alt.Tooltip("mention_count:Q", title="Mentions", format=",d"),
            ],
        )
        .properties(
            height=max(300, hn * 22),
            title=f"Couverture {_LABEL_META[hl]['name']} par chaîne",
        )
        .configure_view(strokeWidth=0, fill="#0f172a")
        .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8")
        .configure_legend(labelColor="#94a3b8", titleColor="#94a3b8")
    )
    st.altair_chart(heatmap, use_container_width=True)
