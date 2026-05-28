# dashboard/views/lead_lag.py
# Feature 7 — Inter-media Lead/Lag Analysis

from __future__ import annotations
from io import StringIO
from datetime import date

import numpy as np
import pandas as pd
import altair as alt
import streamlit as st

from dashboard.data_access import load_keywords_range, load_agenda_gap
from dashboard.ui.components import section_header


def _cross_correlation(
    s1: pd.Series,
    s2: pd.Series,
    max_lag: int = 7,
) -> pd.DataFrame:
    """
    Compute normalized cross-correlation between two daily time series
    for lags in [-max_lag, +max_lag].
    Positive lag = s1 leads s2.
    Returns DataFrame with columns: lag, correlation.
    """
    # Align and fill
    combined = pd.DataFrame({"a": s1, "b": s2}).fillna(0)
    a = combined["a"].values.astype(float)
    b = combined["b"].values.astype(float)

    # Normalize
    a_std = a.std() or 1.0
    b_std = b.std() or 1.0
    a_n = (a - a.mean()) / a_std
    b_n = (b - b.mean()) / b_std

    rows = []
    for lag in range(-max_lag, max_lag + 1):
        if lag == 0:
            corr = float(np.corrcoef(a_n, b_n)[0, 1]) if len(a_n) > 1 else 0.0
        elif lag > 0:
            # s1 leads s2 by lag days
            if lag < len(a_n):
                corr = float(np.corrcoef(a_n[lag:], b_n[:-lag])[0, 1])
            else:
                corr = 0.0
        else:
            # s2 leads s1 by |lag| days
            l = -lag
            if l < len(a_n):
                corr = float(np.corrcoef(a_n[:-l], b_n[l:])[0, 1])
            else:
                corr = 0.0
        rows.append({"lag": lag, "correlation": 0.0 if np.isnan(corr) else corr})

    return pd.DataFrame(rows)


def _daily_word_series(df: pd.DataFrame, source: str, word: str) -> pd.Series:
    sub = df[(df["source"] == source) & (df["word"].str.lower() == word.lower())]
    return sub.set_index("date")["total_count"].sort_index()


def render(filters: dict) -> None:
    start: date = filters["start_date"]
    end: date = filters["end_date"]

    section_header(
        "Inter-media Lead/Lag",
        "Qui parle en premier d'un sujet — TV, presse ou réseaux sociaux ?",
    )

    st.markdown(
        """
        L'analyse **lead/lag** (avance/retard) mesure si un média anticipe la couverture d'un autre
        sur un mot-clé donné. Un **lag positif** signifie que la Source A parle du sujet *avant* la Source B.
        La méthode : corrélation croisée (cross-correlation) des séries temporelles journalières de mentions.
        """
    )

    # ── Controls ──────────────────────────────────────────────────────────────
    ctrl1, ctrl2 = st.columns(2)
    with ctrl1:
        media_type = st.selectbox("Type de media", ["tv", "press"], key="ll_media")
    with ctrl2:
        max_lag = st.slider("Décalage max (jours)", 3, 14, 7, 1, key="ll_maxlag")

    with st.spinner("Chargement des données…"):
        kw_df = load_keywords_range(start, end, media_type=media_type)

    if kw_df.empty:
        st.info("Pas de données mots-clés pour cette période.")
        return

    kw_df["date"] = pd.to_datetime(kw_df["date"])
    all_sources = sorted(kw_df["source"].unique().tolist())
    all_words = sorted(kw_df["word"].str.lower().unique().tolist())

    if len(all_sources) < 2:
        st.info("Au moins deux sources sont nécessaires pour l'analyse lead/lag.")
        return

    st.markdown("---")

    # ── Section A: word-level cross-correlation for a chosen pair ─────────────
    st.markdown("#### Analyse par mot-clé")

    wc1, wc2, wc3 = st.columns(3)
    with wc1:
        src_a = st.selectbox("Source A", all_sources, key="ll_src_a")
    with wc2:
        remaining = [s for s in all_sources if s != src_a]
        src_b = st.selectbox("Source B", remaining, key="ll_src_b")
    with wc3:
        word = st.selectbox("Mot-clé", all_words[:200], key="ll_word")

    ts_a = _daily_word_series(kw_df, src_a, word)
    ts_b = _daily_word_series(kw_df, src_b, word)

    if ts_a.empty or ts_b.empty:
        st.warning(f"Le mot-clé `{word}` n'est pas couvert par l'une des deux sources.")
    else:
        ccf_df = _cross_correlation(ts_a, ts_b, max_lag=max_lag)
        best_lag = int(ccf_df.loc[ccf_df["correlation"].idxmax(), "lag"])
        best_corr = float(ccf_df["correlation"].max())

        m1, m2, m3 = st.columns(3)
        m1.metric("Corrélation max", f"{best_corr:.3f}")
        if best_lag > 0:
            m2.metric("Résultat", f"{src_a} précède {src_b}", f"+{best_lag} jour(s)")
        elif best_lag < 0:
            m2.metric("Résultat", f"{src_b} précède {src_a}", f"{best_lag} jour(s)")
        else:
            m2.metric("Résultat", "Simultané", "lag = 0")
        m3.metric("Fenêtre analysée", f"{(pd.to_datetime(end) - pd.to_datetime(start)).days} jours")

        # Cross-correlation bar chart
        ccf_chart = (
            alt.Chart(ccf_df)
            .mark_bar()
            .encode(
                x=alt.X("lag:Q", title=f"Décalage (jours) — positif = {src_a} précède {src_b}"),
                y=alt.Y("correlation:Q", title="Corrélation", scale=alt.Scale(domain=[-1, 1])),
                color=alt.condition(
                    alt.datum.correlation > 0,
                    alt.value("#6366f1"),
                    alt.value("#ef4444"),
                ),
                tooltip=[
                    alt.Tooltip("lag:Q", title="Décalage (jours)"),
                    alt.Tooltip("correlation:Q", title="Corrélation", format=".3f"),
                ],
            )
            .properties(height=280, title=f"Cross-corrélation : `{word}` — {src_a} vs {src_b}")
            .configure_view(strokeWidth=0, fill="#0f172a")
            .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
        )
        st.altair_chart(ccf_chart, use_container_width=True)

        # Raw time series overlay
        ts_plot = pd.DataFrame({
            "date": pd.date_range(start, end),
        }).merge(
            ts_a.rename("A").reset_index(), on="date", how="left"
        ).merge(
            ts_b.rename("B").reset_index(), on="date", how="left"
        ).fillna(0).melt(id_vars="date", value_vars=["A", "B"],
                         var_name="source", value_name="mentions")
        ts_plot["source"] = ts_plot["source"].map({"A": src_a, "B": src_b})

        lines = (
            alt.Chart(ts_plot)
            .mark_line(point=True, strokeWidth=2)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("mentions:Q", title="Mentions/jour"),
                color=alt.Color("source:N"),
                tooltip=["date:T", "source:N", alt.Tooltip("mentions:Q", format=",d")],
            )
            .properties(height=220, title=f"Évolution journalière : `{word}`")
            .configure_view(strokeWidth=0, fill="#0f172a")
            .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
            .configure_legend(labelColor="#94a3b8", titleColor="#94a3b8")
        )
        st.altair_chart(lines, use_container_width=True)

    # ── Section B: top words with significant lead/lag across all source pairs ──
    st.markdown("---")
    st.markdown("#### Mots-clés avec lead/lag significatif (toutes paires)")

    if st.button("Lancer l'analyse globale (peut prendre ~10s)", key="ll_global_btn"):
        results = []
        top_words_list = (
            kw_df.groupby("word")["total_count"].sum()
            .nlargest(40).index.tolist()
        )

        for w in top_words_list:
            for i, sa in enumerate(all_sources):
                for j, sb in enumerate(all_sources):
                    if j <= i:
                        continue
                    tsa = _daily_word_series(kw_df, sa, w)
                    tsb = _daily_word_series(kw_df, sb, w)
                    if tsa.empty or tsb.empty:
                        continue
                    ccf = _cross_correlation(tsa, tsb, max_lag=max_lag)
                    best = ccf.loc[ccf["correlation"].idxmax()]
                    if abs(best["lag"]) >= 1 and best["correlation"] >= 0.4:
                        results.append({
                            "word": w,
                            "source_a": sa,
                            "source_b": sb,
                            "best_lag": int(best["lag"]),
                            "correlation": round(float(best["correlation"]), 3),
                            "leader": sa if best["lag"] > 0 else sb,
                        })

        if results:
            res_df = pd.DataFrame(results).sort_values("correlation", ascending=False)
            st.dataframe(res_df, use_container_width=True, hide_index=True)

            csv_buf = StringIO()
            res_df.to_csv(csv_buf, index=False)
            st.download_button(
                "⬇️ Exporter analyse lead/lag (CSV)",
                data=csv_buf.getvalue(),
                file_name=f"lead_lag_{start}_{end}.csv",
                mime="text/csv",
            )
        else:
            st.info("Aucun lead/lag significatif détecté (corrélation ≥ 0.4, lag ≥ 1j).")
