# dashboard/views/divergence.py
# Feature 5 — Narrative Divergence Score

from __future__ import annotations
from io import StringIO
from datetime import date

import numpy as np
import pandas as pd
import altair as alt
import streamlit as st

from dashboard.data_access import load_keywords_range, count_articles_by_source, load_dashboard_config
from dashboard.ui.components import section_header, render_confidence


def _divergence_score(df: pd.DataFrame, top_n: int = 50) -> pd.DataFrame:
    """
    Compute Jensen-Shannon-like divergence between source keyword distributions.
    Returns a pairwise (source_a, source_b, score 0-1) DataFrame.
    """
    if df.empty:
        return pd.DataFrame(columns=["source_a", "source_b", "divergence"])

    # Build source × word TF matrix (using total_count)
    pivot = (
        df.groupby(["source", "word"])["total_count"]
        .sum()
        .unstack(fill_value=0)
    )

    # Keep only top_n words by total frequency to avoid noise
    top_words = pivot.sum().nlargest(top_n).index
    pivot = pivot[top_words]

    # Normalize to probability distributions
    row_sums = pivot.sum(axis=1).replace(0, 1)
    prob = pivot.div(row_sums, axis=0)

    sources = list(prob.index)
    rows = []
    for i, sa in enumerate(sources):
        for j, sb in enumerate(sources):
            if j <= i:
                continue
            p = prob.loc[sa].values.astype(float)
            q = prob.loc[sb].values.astype(float)
            # Jensen-Shannon divergence (symmetric, bounded 0-1 when using log2)
            m = (p + q) / 2.0
            with np.errstate(divide="ignore", invalid="ignore"):
                js = 0.5 * np.where(p > 0, p * np.log2(p / np.where(m > 0, m, 1e-12)), 0).sum() \
                   + 0.5 * np.where(q > 0, q * np.log2(q / np.where(m > 0, m, 1e-12)), 0).sum()
            score = float(np.clip(js, 0, 1))
            rows.append({"source_a": sa, "source_b": sb, "divergence": score})

    return pd.DataFrame(rows)


def render(filters: dict) -> None:
    start: date = filters["start_date"]
    end: date = filters["end_date"]

    section_header(
        "Narrative Divergence Score",
        "Distance éditoriale entre chaînes — à quel point couvrent-elles les mêmes sujets ?",
    )

    st.markdown(
        """
        Le **score de divergence** (0 → 1) mesure à quel point deux médias ont des agendas différents
        sur la période. **0** = mêmes mots-clés, même pondération. **1** = couvertures totalement distinctes.
        La méthode utilisée est la divergence de Jensen-Shannon sur les distributions TF de mots-clés.
        """
    )

    ctrl1, ctrl2 = st.columns([2, 2])
    with ctrl1:
        media_type = st.selectbox(
            "Type de media", ["tv", "press"], key="div_media"
        )
    with ctrl2:
        top_n_words = st.slider(
            "Mots-clés pris en compte", 20, 200, 50, 10, key="div_top_words"
        )

    min_n = load_dashboard_config().get("confidence", {}).get("min_n", 8)

    with st.spinner("Calcul des divergences…"):
        kw_df = load_keywords_range(start, end, media_type=media_type)
        counts = count_articles_by_source(start, end, media_type=media_type)
        div_df = _divergence_score(kw_df, top_n=top_n_words)

    if div_df.empty:
        st.info("Pas assez de données pour calculer les divergences.")
        return

    # ── Confidence gating ─────────────────────────────────────────────────────
    if counts:
        min_count = min(counts.values())
        render_confidence(min_count, min_n)

    # ── KPIs ──────────────────────────────────────────────────────────────────
    k1, k2, k3 = st.columns(3)
    k1.metric("Paires analysées", len(div_df))
    k2.metric(
        "Paire la plus divergente",
        f"{div_df.loc[div_df['divergence'].idxmax(), 'source_a']} ↔ "
        f"{div_df.loc[div_df['divergence'].idxmax(), 'source_b']}",
        f"{div_df['divergence'].max():.2f}",
    )
    k3.metric("Score moyen", f"{div_df['divergence'].mean():.2f}")

    st.markdown("---")

    # ── Heatmap ───────────────────────────────────────────────────────────────
    sources = sorted(set(div_df["source_a"]) | set(div_df["source_b"]))
    matrix = pd.DataFrame(0.0, index=sources, columns=sources)
    for _, row in div_df.iterrows():
        matrix.loc[row["source_a"], row["source_b"]] = row["divergence"]
        matrix.loc[row["source_b"], row["source_a"]] = row["divergence"]

    matrix_long = matrix.reset_index().melt(id_vars="index", var_name="source_b", value_name="divergence")
    matrix_long.columns = ["source_a", "source_b", "divergence"]

    heatmap = (
        alt.Chart(matrix_long)
        .mark_rect()
        .encode(
            x=alt.X("source_a:N", title="", axis=alt.Axis(labelAngle=-30)),
            y=alt.Y("source_b:N", title=""),
            color=alt.Color(
                "divergence:Q",
                scale=alt.Scale(scheme="redblue", domain=[0, 1]),
                legend=alt.Legend(title="Divergence"),
            ),
            tooltip=[
                alt.Tooltip("source_a:N", title="Source A"),
                alt.Tooltip("source_b:N", title="Source B"),
                alt.Tooltip("divergence:Q", title="Score", format=".3f"),
            ],
        )
        .properties(height=400, title="Matrice de divergence éditoriale")
        .configure_view(strokeWidth=0, fill="#0f172a")
        .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8")
        .configure_legend(labelColor="#94a3b8", titleColor="#94a3b8")
    )
    st.altair_chart(heatmap, use_container_width=True)

    # ── Ranked bar ────────────────────────────────────────────────────────────
    div_sorted = div_df.sort_values("divergence", ascending=False).copy()
    div_sorted["pair"] = div_sorted["source_a"] + " ↔ " + div_sorted["source_b"]

    bar = (
        alt.Chart(div_sorted)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("divergence:Q", title="Score de divergence", scale=alt.Scale(domain=[0, 1])),
            y=alt.Y("pair:N", sort="-x", title=None),
            color=alt.Color(
                "divergence:Q",
                scale=alt.Scale(scheme="redblue", domain=[0, 1]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("pair:N", title="Paire"),
                alt.Tooltip("divergence:Q", title="Score", format=".3f"),
            ],
        )
        .properties(height=max(250, len(div_sorted) * 26 + 40))
        .configure_view(strokeWidth=0, fill="#0f172a")
        .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
    )
    st.altair_chart(bar, use_container_width=True)

    # ── Shared / exclusive keywords for a pair ────────────────────────────────
    st.markdown("#### Mots-clés partagés vs exclusifs pour une paire")
    all_sources = sorted(kw_df["source"].unique().tolist()) if not kw_df.empty else []
    if len(all_sources) >= 2:
        p_col1, p_col2 = st.columns(2)
        with p_col1:
            src_a = st.selectbox("Source A", all_sources, key="div_src_a")
        with p_col2:
            remaining = [s for s in all_sources if s != src_a]
            src_b = st.selectbox("Source B", remaining, key="div_src_b")

        kw_a = set(kw_df[kw_df["source"] == src_a]["word"].str.lower())
        kw_b = set(kw_df[kw_df["source"] == src_b]["word"].str.lower())
        shared = kw_a & kw_b
        only_a = kw_a - kw_b
        only_b = kw_b - kw_a

        c1, c2, c3 = st.columns(3)
        c1.metric("Partagés", len(shared))
        c2.metric(f"Exclusifs {src_a}", len(only_a))
        c3.metric(f"Exclusifs {src_b}", len(only_b))

        ex1, ex2, ex3 = st.columns(3)
        with ex1:
            st.caption("Partagés (top 10)")
            top_shared = sorted(shared)[:10]
            st.write(", ".join(top_shared) if top_shared else "—")
        with ex2:
            st.caption(f"Exclusifs {src_a} (top 10)")
            st.write(", ".join(sorted(only_a)[:10]) if only_a else "—")
        with ex3:
            st.caption(f"Exclusifs {src_b} (top 10)")
            st.write(", ".join(sorted(only_b)[:10]) if only_b else "—")

    # ── Export ────────────────────────────────────────────────────────────────
    st.markdown("---")
    csv_buf = StringIO()
    div_df.to_csv(csv_buf, index=False)
    st.download_button(
        "⬇️ Exporter scores de divergence (CSV)",
        data=csv_buf.getvalue(),
        file_name=f"divergence_{start}_{end}.csv",
        mime="text/csv",
    )
