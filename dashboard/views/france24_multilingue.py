# dashboard/views/france24_multilingue.py

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.data_access import get_connection


def _read_sql(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn, params=params)


def render(filters: dict):
    st.subheader("🌍 France 24 – Multilingue")

    start_date = filters["start_date"]
    end_date = filters["end_date"]

    days = max(1, (end_date - start_date).days + 1)

    with st.expander("Options (facultatif)", expanded=False):
        include_all = st.toggle("Inclure ALL (agrégé)", value=False)

    st.caption(f"Période: {start_date} → {end_date} ({days} jours)")

    # --- Volume by lang ---
    sql_vol_lang = """
        SELECT lang, SUM(articles_count) AS total_articles
        FROM topics_daily_f24
        WHERE date BETWEEN %s AND %s
        GROUP BY lang
        ORDER BY total_articles DESC;
    """
    df_vol_lang = _read_sql(sql_vol_lang, [start_date, end_date])

    if df_vol_lang.empty:
        st.info("Pas de données France 24 sur la période.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Langues", f"{df_vol_lang['lang'].nunique():,}")
    c2.metric("Articles (topics)", f"{int(df_vol_lang['total_articles'].sum()):,}")
    c3.metric("Période (jours)", f"{days:,}")

    col1, col2 = st.columns([1.05, 1.0], gap="large")

    with col1:
        st.markdown("### 🗣️ Volume par langue")
        chart = (
            alt.Chart(df_vol_lang)
            .mark_bar()
            .encode(
                y=alt.Y("lang:N", sort="-x", title=None),
                x=alt.X("total_articles:Q", title="Articles (somme)"),
                tooltip=["lang:N", "total_articles:Q"],
            )
            .properties(height=260)
        )
        st.altair_chart(chart, width="stretch")

        with st.expander("Détails (table)", expanded=False):
            st.dataframe(df_vol_lang, width="stretch", hide_index=True)

    with col2:
        st.markdown("### 🧠 Top topics par langue")

        sql_top_topics = """
            WITH base AS (
                SELECT date, lang, source, topic_label, articles_count
                FROM topics_daily_f24
                WHERE date BETWEEN %s AND %s
            ),
            filtered AS (
                SELECT *
                FROM base
                WHERE (%s = TRUE) OR (source <> 'ALL')
            ),
            agg AS (
                SELECT
                    lang,
                    topic_label,
                    SUM(articles_count) AS articles_count
                FROM filtered
                GROUP BY lang, topic_label
            )
            SELECT lang, topic_label, articles_count
            FROM (
                SELECT
                    lang,
                    topic_label,
                    articles_count,
                    ROW_NUMBER() OVER (PARTITION BY lang ORDER BY articles_count DESC) AS rn
                FROM agg
            ) t
            WHERE rn <= 8
            ORDER BY lang, articles_count DESC;
        """
        df_top = _read_sql(sql_top_topics, [start_date, end_date, include_all])

        if df_top.empty:
            st.info("Pas assez de topics F24 sur la période.")
        else:
            base = (
                alt.Chart(df_top)
                .mark_bar()
                .encode(
                    y=alt.Y("topic_label:N", sort="-x", title=None),
                    x=alt.X("articles_count:Q", title="Articles"),
                    tooltip=["lang:N", "topic_label:N", "articles_count:Q"],
                )
                .properties(height=220)  # ✅ height belongs to the inner spec
            )

            bars = (
                base.facet(
                    row=alt.Row(
                        "lang:N",
                        sort=alt.SortField(field="lang", order="ascending"),
                        header=alt.Header(title=None, labelAngle=0, labelPadding=8),
                    )
                )
                .resolve_scale(y="independent")  # ✅ each language gets its own y categories
            )

            st.altair_chart(bars, width="stretch")



            with st.expander("Détails (table)", expanded=False):
                st.dataframe(df_top, width="stretch", hide_index=True)
