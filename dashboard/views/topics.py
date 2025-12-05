# dashboard/views/topics.py

from datetime import date as date_type

import streamlit as st
import pandas as pd
import altair as alt

from data_access import (
    get_available_dates,
    load_word_trend,
    load_topics_for_day,
)


def render():
    st.title("🧠 Exploration des sujets & narratifs")

    dates = get_available_dates()
    if not dates:
        st.error("Aucune donnée disponible.")
        return

    min_date, max_date = dates[0], dates[-1]

    with st.sidebar:
        st.header("Filtres sujets & mots-clés")

        start_date, end_date = st.date_input(
            "Période",
            value=(max_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(start_date, list) or isinstance(start_date, tuple):
            start_date, end_date = start_date

        focus_word = st.text_input(
            "Mot-clé à analyser (optionnel, ex: sécurité, budget, immigration, énergie)",
            value="sécurité",
        )

        selected_date_topics: date_type = st.date_input(
            "Date pour l'exploration détaillée des topics",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key="topics_date",
        )

    col1, col2 = st.columns(2)

    # --- Colonne 1 : tendance d'un mot-clé ---
    with col1:
        st.subheader(f"Tendance du mot-clé : `{focus_word}`")

        if focus_word.strip():
            df_trend = load_word_trend(focus_word.strip(), start_date, end_date, media_type="tv")

            if df_trend.empty:
                st.info("Ce mot-clé ne semble pas apparaître dans la période sélectionnée.")
            else:
                df_trend["date"] = df_trend["date"].dt.date

                line = (
                    alt.Chart(df_trend)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("date:T", title="Date"),
                        y=alt.Y("total_mentions:Q", title="Nb. de mentions"),
                        color=alt.Color("source:N", title="Chaîne"),
                        tooltip=["date", "source", "total_mentions"],
                    )
                    .properties(height=300)
                )
                st.altair_chart(line, use_container_width=True)

                st.dataframe(
                    df_trend[["date", "source", "total_mentions"]],
                    use_container_width=True,
                    hide_index=True,
                )
        else:
            st.info("Saisissez un mot-clé dans la barre latérale pour voir sa tendance.")

    # --- Colonne 2 : topics détaillés pour une date ---
    with col2:
        st.subheader(f"Sujets détaillés – TV – {selected_date_topics}")

        df_topics = load_topics_for_day(selected_date_topics, only_tv=True)
        if df_topics.empty:
            st.info("Pas de sujets pour cette date.")
        else:
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
