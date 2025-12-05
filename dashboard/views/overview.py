# dashboard/views/overview.py

from datetime import date as date_type

import streamlit as st
import pandas as pd
import altair as alt

from data_access import (
    get_available_dates,
    get_sources,
    load_keywords_for_day,
    load_topics_for_day,
)


def render():
    st.title("📺 Vue d'ensemble – Media Agenda du jour")

    dates = get_available_dates()
    if not dates:
        st.error("Aucune donnée disponible dans keywords_daily.")
        return

    min_date, max_date = dates[0], dates[-1]

    with st.sidebar:
        st.header("Filtres")
        selected_date: date_type = st.date_input(
            "Date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
        )

        sources = get_sources()
        selected_source = st.selectbox("Chaîne (source)", options=sources, index=0)

    col1, col2 = st.columns(2)

    # --- Colonne 1 : Top mots-clés ---
    with col1:
        st.subheader(f"Top mots-clés – {selected_source} – {selected_date}")

        df_kw = load_keywords_for_day(selected_date, selected_source, media_type=None)

        if df_kw.empty:
            st.info("Pas de mots-clés pour cette date / source.")
        else:
            # Affichage table
            st.dataframe(
                df_kw[["rank", "word", "count", "source", "media_type"]],
                use_container_width=True,
                hide_index=True,
            )

            # Bar chart Altair
            chart = (
                alt.Chart(df_kw)
                .mark_bar()
                .encode(
                    x=alt.X("word:N", sort="-y", title="Mot"),
                    y=alt.Y("count:Q", title="Occurrences"),
                    tooltip=["word", "count", "source", "media_type"],
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)

    # --- Colonne 2 : Sujets du jour ---
    with col2:
        st.subheader(f"Sujets dominants – TV – {selected_date}")

        df_topics = load_topics_for_day(selected_date, only_tv=True)
        if df_topics.empty:
            st.info("Pas de sujets pour cette date.")
        else:
            # On trie par importance (articles_count)
            df_topics = df_topics.sort_values("articles_count", ascending=False)

            for _, row in df_topics.iterrows():
                with st.expander(
                    f"Topic {int(row['topic_id'])} – {row['topic_label']} "
                    f"(Articles : {int(row['articles_count'])})",
                    expanded=False,
                ):
                    keywords = row["keywords"]
                    if isinstance(keywords, (list, tuple)):
                        kw_text = ", ".join(keywords)
                    else:
                        kw_text = str(keywords)
                    st.markdown(f"**Mots-clés :** {kw_text}")
