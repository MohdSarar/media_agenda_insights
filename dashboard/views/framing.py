# dashboard/views/framing.py
# Feature 6 — Framing Analysis

from __future__ import annotations
from io import StringIO
from datetime import date
from collections import defaultdict

import pandas as pd
import altair as alt
import streamlit as st

from dashboard.data_access import load_keywords_range
from dashboard.ui.components import section_header


# ── Frame lexicons (French + some English for press) ─────────────────────────
FRAMES: dict[str, list[str]] = {
    "Sécurité / Ordre": [
        "police", "sécurité", "crime", "terrorisme", "violence", "attentat",
        "prison", "garde", "armée", "gendarme", "justice", "tribunal",
        "condamné", "arrestation", "meurtrier", "criminel", "menace",
        "trafic", "drogue", "frontière", "surveillance", "contrôle",
    ],
    "Économie / Travail": [
        "économie", "emploi", "chômage", "salaire", "budget", "croissance",
        "inflation", "marché", "entreprise", "investissement", "dette",
        "finance", "industrie", "commerce", "exportation", "coût",
        "récession", "bourse", "banque", "pib", "revenu", "fiscalité",
    ],
    "Humanitaire / Social": [
        "refuge", "humanitaire", "pauvreté", "aide", "solidarité",
        "migrant", "réfugié", "famille", "enfant", "santé", "hôpital",
        "logement", "précarité", "association", "bénévole", "don",
        "crise", "victime", "protection", "droit", "égalité",
    ],
    "Politique / Gouvernance": [
        "gouvernement", "président", "ministre", "parlement", "loi",
        "réforme", "parti", "élection", "vote", "assemblée", "sénat",
        "coalition", "opposition", "pouvoir", "décret", "politique",
        "démocratie", "candidat", "campagne", "mandat", "institution",
    ],
    "Environnement / Climat": [
        "climat", "environnement", "énergie", "réchauffement", "pollution",
        "carbone", "forêt", "biodiversité", "écologie", "renouvelable",
        "sécheresse", "inondation", "transition", "dioxyde", "gaz",
        "nucléaire", "solaire", "éolien", "catastrophe", "nature",
    ],
    "Conflit / Guerre": [
        "guerre", "conflit", "armée", "bombardement", "offensive",
        "militaire", "soldats", "cessez-le-feu", "otan", "ukraine",
        "israël", "hamas", "frappes", "missiles", "blessés", "morts",
        "civils", "siège", "résistance", "coalition", "occupation",
    ],
}

FRAME_COLORS = [
    "#6366f1", "#10b981", "#f59e0b", "#3b82f6", "#22d3ee", "#ef4444"
]


def _score_frames(
    kw_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each (source, frame) compute coverage = sum of keyword counts
    where the keyword appears in the frame's lexicon.
    Returns: source, frame, coverage, coverage_norm.
    """
    rows = []
    for (src,), grp in kw_df.groupby(["source"]):
        word_counts = dict(zip(grp["word"].str.lower(), grp["total_count"]))
        for frame, lexicon in FRAMES.items():
            score = sum(word_counts.get(w, 0) for w in lexicon)
            rows.append({"source": src, "frame": frame, "coverage": score})

    if not rows:
        return pd.DataFrame(columns=["source", "frame", "coverage", "coverage_norm"])

    df = pd.DataFrame(rows)

    # Normalize per-source so 0-1 within each source
    def norm(g):
        mx = g["coverage"].max() or 1
        g = g.copy()
        g["coverage_norm"] = g["coverage"] / mx
        return g

    df = df.groupby("source", group_keys=False).apply(norm)
    return df


def render(filters: dict) -> None:
    start: date = filters["start_date"]
    end: date = filters["end_date"]

    section_header(
        "Framing Analysis",
        "Comment chaque média cadre sa couverture — sécuritaire, économique, humanitaire…",
    )

    st.markdown(
        """
        L'analyse de **cadrage** (*framing*) détecte quel prisme éditorial domine dans la couverture
        d'un sujet. Chaque **frame** est détectée par la présence de mots-clés caractéristiques
        dans les lemmes de la période.
        """
    )

    ctrl1, ctrl2 = st.columns([2, 2])
    with ctrl1:
        media_type = st.selectbox("Type de media", ["tv", "press", "ALL"], key="frame_media")
    with ctrl2:
        filter_source = st.multiselect(
            "Filtrer les sources (vide = toutes)",
            options=[],  # populated after data load
            key="frame_sources",
        )

    mt = None if media_type == "ALL" else media_type

    with st.spinner("Analyse du framing…"):
        kw_df = load_keywords_range(start, end, media_type=mt or "tv")

    if kw_df.empty:
        st.info("Pas de données mots-clés pour cette période.")
        return

    # Populate source filter now that we have data
    all_sources = sorted(kw_df["source"].unique().tolist())
    if filter_source:
        kw_df = kw_df[kw_df["source"].isin(filter_source)]

    frame_df = _score_frames(kw_df)

    if frame_df.empty or frame_df["coverage"].sum() == 0:
        st.info("Aucun mot-clé de framing détecté sur cette période.")
        return

    st.markdown("---")

    # ── Radar / Spider chart via Altair (polar approximation using bar+coord_polar) ──
    # Altair doesn't natively do radar, so we use a stacked horizontal bar per frame
    col_main, col_legend = st.columns([4, 1])

    with col_main:
        st.markdown("#### Répartition des frames par source")

        stacked = (
            alt.Chart(frame_df[frame_df["coverage"] > 0])
            .mark_bar()
            .encode(
                x=alt.X("coverage:Q", title="Couverture (occurrences cumulées)", stack="normalize"),
                y=alt.Y("source:N", title="Source", sort="-x"),
                color=alt.Color(
                    "frame:N",
                    scale=alt.Scale(
                        domain=list(FRAMES.keys()),
                        range=FRAME_COLORS,
                    ),
                    legend=alt.Legend(title="Frame", orient="bottom", columns=2),
                ),
                tooltip=[
                    alt.Tooltip("source:N", title="Source"),
                    alt.Tooltip("frame:N", title="Frame"),
                    alt.Tooltip("coverage:Q", title="Occurrences", format=",d"),
                    alt.Tooltip("coverage_norm:Q", title="Intensité", format=".2f"),
                ],
            )
            .properties(height=max(300, len(all_sources) * 35 + 60))
            .configure_view(strokeWidth=0, fill="#0f172a")
            .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8")
            .configure_legend(labelColor="#94a3b8", titleColor="#94a3b8")
        )
        st.altair_chart(stacked, use_container_width=True)

    # ── Per-frame heatmap (source × frame intensity) ──────────────────────────
    st.markdown("#### Intensité par frame et par source")

    heatmap_df = frame_df.pivot(index="source", columns="frame", values="coverage_norm").fillna(0)
    heatmap_long = heatmap_df.reset_index().melt(id_vars="source", var_name="frame", value_name="intensity")

    hm = (
        alt.Chart(heatmap_long)
        .mark_rect()
        .encode(
            x=alt.X("frame:N", title=None, axis=alt.Axis(labelAngle=-30, labelLimit=120)),
            y=alt.Y("source:N", title=None),
            color=alt.Color(
                "intensity:Q",
                scale=alt.Scale(scheme="purpleblue", domain=[0, 1]),
                legend=alt.Legend(title="Intensité"),
            ),
            tooltip=[
                alt.Tooltip("source:N", title="Source"),
                alt.Tooltip("frame:N", title="Frame"),
                alt.Tooltip("intensity:Q", title="Intensité", format=".2f"),
            ],
        )
        .properties(height=max(200, len(all_sources) * 30))
        .configure_view(strokeWidth=0, fill="#0f172a")
        .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8")
        .configure_legend(labelColor="#94a3b8", titleColor="#94a3b8")
    )
    st.altair_chart(hm, use_container_width=True)

    # ── Dominant frame per source ─────────────────────────────────────────────
    st.markdown("#### Frame dominante par source")
    dominant = (
        frame_df.sort_values("coverage", ascending=False)
        .groupby("source")
        .first()
        .reset_index()[["source", "frame", "coverage"]]
    )
    dominant.columns = ["Source", "Frame dominante", "Occurrences"]
    st.dataframe(dominant, use_container_width=True, hide_index=True)

    # ── Lexicon editor ────────────────────────────────────────────────────────
    with st.expander("📝 Voir / modifier les lexiques de framing", expanded=False):
        for fname, lexicon in FRAMES.items():
            st.markdown(f"**{fname}**")
            st.caption(", ".join(lexicon))

    # ── Export ────────────────────────────────────────────────────────────────
    st.markdown("---")
    csv_buf = StringIO()
    frame_df.to_csv(csv_buf, index=False)
    st.download_button(
        "⬇️ Exporter analyse framing (CSV)",
        data=csv_buf.getvalue(),
        file_name=f"framing_{start}_{end}.csv",
        mime="text/csv",
    )
