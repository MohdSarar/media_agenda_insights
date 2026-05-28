# dashboard/views/france24_multilingue.py

import pandas as pd
import streamlit as st
import altair as alt

from dashboard.data_access import get_connection
from dashboard.ui.components import section_header


def _load(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _chart_volume_by_lang(df: pd.DataFrame) -> alt.Chart:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("articles_count:Q", title="Volume (articles - topics)"),
            color=alt.Color("lang:N", title="Langue"),
            tooltip=["date:T", "lang:N", "articles_count:Q"],
        )
        .properties(height=280)
    )


def _chart_volume_by_source(df: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("articles_count:Q", title="Volume"),
            y=alt.Y("source:N", sort="-x", title="Version France 24"),
            color=alt.Color("source:N", legend=None),
            tooltip=["source:N", "articles_count:Q"],
        )
        .properties(height=200)
    )


def _chart_top_topics_grouped(df: pd.DataFrame) -> alt.Chart:
    """
    Faceted bar chart — one panel per language with independent Y axes.
    This ensures topics from each language are grouped separately and not dispersed.
    """
    base = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("articles_count:Q", title="Nb d'articles"),
            y=alt.Y("topic_label:N", sort="-x", title=None),
            color=alt.Color("lang:N", legend=None),
            tooltip=["lang:N", "topic_label:N", "articles_count:Q"],
        )
        .properties(height=220)
    )

    return (
        base.facet(
            row=alt.Row(
                "lang:N",
                title=None,
                header=alt.Header(
                    labelAngle=0,
                    labelPadding=10,
                    labelFontSize=13,
                    labelFontWeight="bold",
                    labelColor="#94a3b8",
                ),
            )
        )
        .resolve_scale(y="independent")   # ← key fix: each language has its own Y axis
        .properties(bounds="flush")
        .configure_facet(spacing=20)
    )


def render(filters: dict):
    section_header(
        "France 24 Multilingue — FR / EN / ES / AR",
        "Comparaison éditoriale entre les versions linguistiques",
    )

    # Inline controls (no sidebar)
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        days = st.selectbox("Fenêtre d'analyse", [3, 7, 14, 30], index=1, key="f24_days")
    with c2:
        include_all = st.toggle("Inclure source='ALL'", value=False, key="f24_all")
    with c3:
        show_tables = st.toggle("Afficher les tables détaillées", value=False, key="f24_tables")

    params = {"days": days, "include_all": include_all}

    # ── Volume par langue ─────────────────────────────────────────────────────
    df_vol_lang = _load("""
        SELECT date, lang, SUM(articles_count) AS articles_count
        FROM topics_daily_f24
        WHERE date >= CURRENT_DATE - %(days)s
          AND (%(include_all)s = TRUE OR source <> 'ALL')
        GROUP BY date, lang ORDER BY date ASC;
    """, params)

    # ── Volume par source ─────────────────────────────────────────────────────
    df_vol_source = _load("""
        SELECT source, SUM(articles_count) AS articles_count
        FROM topics_daily_f24
        WHERE date >= CURRENT_DATE - %(days)s
          AND (%(include_all)s = TRUE OR source <> 'ALL')
        GROUP BY source ORDER BY articles_count DESC;
    """, params)

    # ── Top 8 topics par langue ───────────────────────────────────────────────
    df_top_topics = _load("""
        WITH agg AS (
            SELECT lang, topic_label, SUM(articles_count) AS articles_count
            FROM topics_daily_f24
            WHERE date >= CURRENT_DATE - %(days)s
              AND (%(include_all)s = TRUE OR source <> 'ALL')
              AND topic_label IS NOT NULL AND topic_label <> ''
            GROUP BY lang, topic_label
        )
        SELECT * FROM (
            SELECT lang, topic_label, articles_count,
                   ROW_NUMBER() OVER (PARTITION BY lang ORDER BY articles_count DESC) AS rn
            FROM agg
        ) t
        WHERE rn <= 8
        ORDER BY lang, articles_count DESC;
    """, params)

    # ── Charts ────────────────────────────────────────────────────────────────
    st.markdown("### Comparatifs éditoriaux")

    if df_vol_lang.empty:
        st.info("Pas assez de données topics sur la période.")
    else:
        st.subheader("Volume par langue dans le temps")
        st.altair_chart(_chart_volume_by_lang(df_vol_lang), use_container_width=True)

    if not df_vol_source.empty:
        st.subheader("Répartition par version (source)")
        st.altair_chart(_chart_volume_by_source(df_vol_source), use_container_width=True)

    if not df_top_topics.empty:
        st.subheader("Top topics par langue (Top 8)")
        st.altair_chart(_chart_top_topics_grouped(df_top_topics), use_container_width=True)

    # ── Tables optionnelles ───────────────────────────────────────────────────
    if show_tables:
        st.markdown("### Tables détaillées")

        with st.expander("Topics par langue (200 dernières lignes)"):
            dft = _load("""
                SELECT date, source, lang, topic_id, topic_label, articles_count, keywords
                FROM topics_daily_f24
                WHERE date >= CURRENT_DATE - %(days)s
                  AND (%(include_all)s = TRUE OR source <> 'ALL')
                ORDER BY date DESC, articles_count DESC LIMIT 200;
            """, params)
            st.dataframe(dft, use_container_width=True)

        with st.expander("Keywords par langue (200 dernières lignes)"):
            dfk = _load("""
                SELECT date, source, lang, word, count, rank
                FROM keywords_daily_f24
                WHERE date >= CURRENT_DATE - %(days)s
                ORDER BY date DESC, source, rank ASC LIMIT 200;
            """, {"days": days})
            st.dataframe(dfk, use_container_width=True)
