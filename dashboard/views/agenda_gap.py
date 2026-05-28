# dashboard/views/agenda_gap.py

from __future__ import annotations
from io import StringIO
from datetime import date

import pandas as pd
import altair as alt
import streamlit as st

from dashboard.data_access import load_agenda_gap
from dashboard.ui.components import section_header


_QUADRANT_COLORS = {
    "Sur les réseaux, ignoré TV": "#f59e0b",
    "Couverture massive": "#10b981",
    "Signal faible": "#475569",
    "Sujet TV, absent réseaux": "#6366f1",
}


def _classify(row: pd.Series) -> str:
    tv, soc = row["tv_norm"], row["social_norm"]
    if tv >= 0.4 and soc >= 0.4:
        return "Couverture massive"
    if tv >= 0.4 and soc < 0.4:
        return "Sujet TV, absent réseaux"
    if tv < 0.4 and soc >= 0.4:
        return "Sur les réseaux, ignoré TV"
    return "Signal faible"


def render(filters: dict) -> None:
    start: date = filters["start_date"]
    end: date = filters["end_date"]

    section_header(
        "Agenda Gap Detector",
        "Sujets sur-couverts par un media et ignorés par l'autre",
    )

    with st.spinner("Chargement des données agenda gap…"):
        df = load_agenda_gap(start, end)

    if df.empty:
        st.info(
            "Aucune donnée disponible pour la période sélectionnée. "
            "Vérifiez que les tables keywords_daily et social_keywords_daily contiennent des données."
        )
        return

    df["quadrant"] = df.apply(_classify, axis=1)

    # ── KPIs ──────────────────────────────────────────────────────────────────
    counts = df["quadrant"].value_counts()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📺 Sujet TV ignoré réseaux", counts.get("Sujet TV, absent réseaux", 0))
    c2.metric("📱 Viral réseaux, ignoré TV", counts.get("Sur les réseaux, ignoré TV", 0))
    c3.metric("🔥 Couverture massive", counts.get("Couverture massive", 0))
    c4.metric("🔕 Signal faible", counts.get("Signal faible", 0))

    st.markdown("---")

    # ── Controls ──────────────────────────────────────────────────────────────
    col_ctrl1, col_ctrl2, col_ctrl3 = st.columns([2, 2, 3])
    with col_ctrl1:
        selected_quadrants = st.multiselect(
            "Filtrer par quadrant",
            options=list(_QUADRANT_COLORS.keys()),
            default=list(_QUADRANT_COLORS.keys()),
        )
    with col_ctrl2:
        min_signal = st.slider(
            "Signal minimum (TV ou Social, 0-1)",
            0.0, 1.0, 0.0, 0.05,
        )
    with col_ctrl3:
        top_n = st.slider("Nombre de mots-clés affichés", 20, 300, 100, 10)

    plot_df = df[df["quadrant"].isin(selected_quadrants)].copy()
    plot_df = plot_df[
        (plot_df["tv_norm"] >= min_signal) | (plot_df["social_norm"] >= min_signal)
    ].head(top_n)

    if plot_df.empty:
        st.warning("Aucun mot-clé ne correspond aux filtres sélectionnés.")
        return

    # ── Scatter plot ──────────────────────────────────────────────────────────
    color_scale = alt.Scale(
        domain=list(_QUADRANT_COLORS.keys()),
        range=list(_QUADRANT_COLORS.values()),
    )

    scatter = (
        alt.Chart(plot_df)
        .mark_circle(opacity=0.85)
        .encode(
            x=alt.X(
                "tv_norm:Q",
                title="Couverture TV (normalisée)",
                axis=alt.Axis(grid=True, gridColor="#334155"),
                scale=alt.Scale(domain=[0, 1]),
            ),
            y=alt.Y(
                "social_norm:Q",
                title="Buzz Social (normalisé)",
                axis=alt.Axis(grid=True, gridColor="#334155"),
                scale=alt.Scale(domain=[0, 1]),
            ),
            size=alt.Size(
                "tv_count:Q",
                scale=alt.Scale(range=[40, 600]),
                legend=alt.Legend(title="Mentions TV"),
            ),
            color=alt.Color(
                "quadrant:N",
                scale=color_scale,
                legend=alt.Legend(title="Quadrant", orient="bottom", columns=2),
            ),
            tooltip=[
                alt.Tooltip("keyword:N", title="Mot-clé"),
                alt.Tooltip("tv_count:Q", title="Mentions TV", format=",d"),
                alt.Tooltip("social_score:Q", title="Score Social", format=".1f"),
                alt.Tooltip("tv_norm:Q", title="TV normalisé", format=".2f"),
                alt.Tooltip("social_norm:Q", title="Social normalisé", format=".2f"),
                alt.Tooltip("quadrant:N", title="Quadrant"),
            ],
        )
    )

    # Quadrant dividing lines at 0.4
    h_line = (
        alt.Chart(pd.DataFrame({"y": [0.4]}))
        .mark_rule(color="#475569", strokeDash=[6, 4], strokeWidth=1.5)
        .encode(y="y:Q")
    )
    v_line = (
        alt.Chart(pd.DataFrame({"x": [0.4]}))
        .mark_rule(color="#475569", strokeDash=[6, 4], strokeWidth=1.5)
        .encode(x="x:Q")
    )

    # Quadrant labels
    label_data = pd.DataFrame(
        [
            {"x": 0.12, "y": 0.92, "label": "📱 Viral réseaux\nignoré TV"},
            {"x": 0.72, "y": 0.92, "label": "🔥 Couverture\nmassive"},
            {"x": 0.12, "y": 0.08, "label": "🔕 Signal\nfaible"},
            {"x": 0.72, "y": 0.08, "label": "📺 Sujet TV\nabsent réseaux"},
        ]
    )
    labels = (
        alt.Chart(label_data)
        .mark_text(fontSize=11, color="#64748b", fontStyle="italic")
        .encode(x="x:Q", y="y:Q", text="label:N")
    )

    chart = (
        (scatter + h_line + v_line + labels)
        .properties(height=520, title="")
        .configure_view(strokeWidth=0, fill="#0f172a")
        .configure_axis(
            labelColor="#94a3b8",
            titleColor="#94a3b8",
            domainColor="#334155",
        )
        .configure_legend(
            labelColor="#94a3b8",
            titleColor="#94a3b8",
        )
        .interactive()
    )

    st.altair_chart(chart, use_container_width=True)

    # ── Detail tables per quadrant ────────────────────────────────────────────
    st.markdown("#### Détail par quadrant")

    interesting = ["Sur les réseaux, ignoré TV", "Sujet TV, absent réseaux", "Couverture massive"]
    col_left, col_right = st.columns(2)
    panels = [col_left, col_right, col_left]

    for col, qname in zip(panels, interesting):
        sub = (
            plot_df[plot_df["quadrant"] == qname]
            [["keyword", "tv_count", "social_score", "tv_norm", "social_norm"]]
            .sort_values("tv_count" if "TV" in qname else "social_score", ascending=False)
            .head(15)
            .reset_index(drop=True)
        )
        sub.index += 1
        with col:
            color = _QUADRANT_COLORS.get(qname, "#6366f1")
            st.markdown(
                f'<p style="color:{color};font-weight:600;margin-bottom:0.25rem;">'
                f"{qname}</p>",
                unsafe_allow_html=True,
            )
            st.dataframe(sub, use_container_width=True, height=280)

    # ── CSV Export ────────────────────────────────────────────────────────────
    st.markdown("---")
    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    st.download_button(
        label="⬇️ Exporter toutes les données (CSV)",
        data=csv_buf.getvalue(),
        file_name=f"agenda_gap_{start}_{end}.csv",
        mime="text/csv",
    )
