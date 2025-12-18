# dashboard/views/overview.py

from __future__ import annotations

from datetime import date as date_type

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.data_access import (
    load_keywords_for_day,
    load_topics_for_day,
)


def render(filters: dict):
    """Compact 'today/last day' overview (no sidebar, uses global filters)."""
    end_date: date_type = filters["end_date"]
    source: str = filters.get("source", "ALL")
    media_type = filters.get("media_type", None)  # None = all

    st.subheader(f"📌 Snapshot – {end_date}")

    # --- Load data (day-based) ---
    df_kw = load_keywords_for_day(end_date, selected_source=source, media_type=media_type)
    df_topics = load_topics_for_day(end_date, only_tv=True)  # topics_daily is TV-focused in your pipeline

    # --- KPIs ---
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Mots-clés", f"{len(df_kw):,}")
    with c2:
        st.metric("Topics (TV)", f"{len(df_topics):,}")
    with c3:
        top_word = df_kw.sort_values("count", ascending=False)["word"].iloc[0] if not df_kw.empty else "—"
        st.metric("Mot #1", top_word)
    with c4:
        top_topic = df_topics.sort_values("articles_count", ascending=False)["topic_label"].iloc[0] if not df_topics.empty else "—"
        st.metric("Topic #1", top_topic)

    st.divider()

    col1, col2 = st.columns([1.2, 1.0], gap="large")

    # --- Left: Keywords chart + table (compact) ---
    with col1:
        st.markdown("### 🔤 Top mots-clés (jour)")
        if df_kw.empty:
            st.info("Aucun mot-clé pour ce jour avec ces filtres.")
        else:
            # Keep top 20 for readability
            df_top = df_kw.sort_values("count", ascending=False).head(20).copy()

            chart = (
                alt.Chart(df_top)
                .mark_bar()
                .encode(
                    x=alt.X("word:N", sort="-y", title=None),
                    y=alt.Y("count:Q", title="Occurrences"),
                    tooltip=["word", "count", "source", "media_type"],
                )
                .properties(height=320)
            )
            st.altair_chart(chart, width="stretch")

            with st.expander("Détails (table)", expanded=False):
                st.dataframe(
                    df_top[["source", "media_type", "word", "count", "rank"]],
                    width="stretch",
                    hide_index=True,
                )

    # --- Right: Topics list (TV) ---
    with col2:
        st.markdown("### 🧠 Sujets dominants (TV)")
        if df_topics.empty:
            st.info("Pas de sujets pour cette date.")
        else:
            df_topics = df_topics.sort_values("articles_count", ascending=False).head(12)

            for _, row in df_topics.iterrows():
                title = f"Topic {int(row['topic_id'])} — {row['topic_label']} ({int(row['articles_count'])} articles)"
                with st.expander(title, expanded=False):
                    keywords = row.get("keywords")
                    if isinstance(keywords, (list, tuple)):
                        kw_text = ", ".join(keywords)
                    else:
                        kw_text = str(keywords)
                    st.caption("Mots-clés")
                    st.write(kw_text)
