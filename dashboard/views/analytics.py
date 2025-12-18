# dashboard/views/analytics.py

from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard.data_access import get_connection


def _read_sql(query: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(query, conn, params=params)


def render(filters: dict):
    st.subheader("📐 Analytics (tables analytiques)")

    start_date = filters["start_date"]
    end_date = filters["end_date"]

    analysis = st.selectbox(
        "Analyse",
        [
            "Media Bias",
            "Topic Spikes",
            "Keyword Lifetime",
            "Topic Lifetime",
            "Theme Lifetime",
        ],
        index=0,
    )

    if analysis == "Media Bias":
        st.markdown("### 📌 Media Bias Scores (par thème et source)")

        query = """
            SELECT
                date,
                source,
                theme,
                AVG(bias_score)    AS bias_score,
                MIN(methodology)  AS methodology,
                COUNT(*)          AS n_obs
            FROM media_bias_scores
            WHERE date BETWEEN %s AND %s
            GROUP BY date, source, theme
            ORDER BY date DESC, source, theme
            LIMIT 2000;
        """
        df = _read_sql(query, params=[start_date, end_date])

        if df.empty:
            st.info("Aucun score de biais sur la période.")
            return

        with st.expander("Filtres (facultatif)", expanded=False):
            sources = ["ALL"] + sorted(df["source"].dropna().unique().tolist())
            themes = ["ALL"] + sorted(df["theme"].dropna().unique().tolist())
            src = st.selectbox("Source", sources, index=0)
            th = st.selectbox("Thème", themes, index=0)

        if src != "ALL":
            df = df[df["source"] == src]
        if th != "ALL":
            df = df[df["theme"] == th]

        c1, c2, c3 = st.columns(3)
        c1.metric("Lignes", f"{len(df):,}")
        c2.metric("Sources", f"{df['source'].nunique():,}")
        c3.metric("Thèmes", f"{df['theme'].nunique():,}")

        st.dataframe(df.sort_values("date", ascending=False), width="stretch", hide_index=True)

        st.markdown("#### Moyenne des biais (source × thème)")
        summary = (
            df.groupby(["source", "theme"], as_index=False)["bias_score"]
            .mean()
            .sort_values("bias_score", ascending=False)
        )
        st.dataframe(summary, width="stretch", hide_index=True)

    elif analysis == "Topic Spikes":
        st.markdown("### ⚡ Topic Spikes")

        query = """
            SELECT date, source, topic_label, spike_score, z_score, baseline, current_value
            FROM topic_spikes
            WHERE date BETWEEN %s AND %s
            ORDER BY date DESC, spike_score DESC
            LIMIT 2000
        """
        df = _read_sql(query, params=[start_date, end_date])

        if df.empty:
            st.info("Aucun spike détecté sur la période.")
            return

        with st.expander("Filtres (facultatif)", expanded=False):
            sources = ["ALL"] + sorted(df["source"].dropna().unique().tolist())
            src = st.selectbox("Source", sources, index=0)
            topk = st.slider("Top K", 10, 200, 50, step=10)

        if src != "ALL":
            df = df[df["source"] == src]

        df = df.sort_values("spike_score", ascending=False).head(topk)
        st.dataframe(df, width="stretch", hide_index=True)

    elif analysis == "Keyword Lifetime":
        st.markdown("### ⏳ Keyword Lifetime")

        query = """
            SELECT word, start_date, end_date, duration_days, total_frequency
            FROM keyword_lifetime
            WHERE start_date <= %s AND end_date >= %s
            ORDER BY duration_days DESC, total_frequency DESC
            LIMIT 2000
        """
        df = _read_sql(query, params=[end_date, start_date])

        if df.empty:
            st.info("Aucun lifetime keyword disponible.")
            return

        with st.expander("Options (facultatif)", expanded=False):
            min_days = st.slider("Durée min (jours)", 1, 60, 7)
            df = df[df["duration_days"] >= min_days]

        st.dataframe(df, width="stretch", hide_index=True)

    elif analysis == "Topic Lifetime":
        st.markdown("### ⏳ Topic Lifetime")

        query = """
            SELECT topic_label, start_date, end_date, duration_days, total_articles
            FROM topic_lifetime
            WHERE start_date <= %s AND end_date >= %s
            ORDER BY duration_days DESC, total_articles DESC
            LIMIT 2000
        """
        df = _read_sql(query, params=[end_date, start_date])

        if df.empty:
            st.info("Aucun lifetime topic disponible.")
            return

        st.dataframe(df, width="stretch", hide_index=True)

    elif analysis == "Theme Lifetime":
        st.markdown("### ⏳ Theme Lifetime")

        query = """
            SELECT theme, start_date, end_date, duration_days, total_score
            FROM theme_lifetime
            WHERE start_date <= %s AND end_date >= %s
            ORDER BY duration_days DESC, total_score DESC
            LIMIT 2000
        """
        df = _read_sql(query, params=[end_date, start_date])

        if df.empty:
            st.info("Aucun lifetime theme disponible.")
            return

        st.dataframe(df, width="stretch", hide_index=True)
