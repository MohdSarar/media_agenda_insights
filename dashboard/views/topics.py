# dashboard/views/topics.py

import streamlit as st
import altair as alt

from dashboard.data_access import load_word_trend, load_topics_for_day
from dashboard.ui.components import section_header


def render(filters: dict):
    start_date = filters["start_date"]
    end_date = filters["end_date"]

    section_header(
        "Exploration des sujets & narratifs",
        f"Tendances de mots-clés et sujets détaillés du {start_date} au {end_date}",
    )

    c_kw, c_date = st.columns([3, 2])
    with c_kw:
        focus_word = st.text_input(
            "Mot-clé à analyser",
            value="sécurité",
            placeholder="ex: budget, ukraine, énergie…",
        )
    with c_date:
        selected_date_topics = st.date_input(
            "Date pour l'exploration détaillée",
            value=end_date,
            min_value=filters["db_min"],
            max_value=filters["db_max"],
            key="topics_detail_date",
        )

    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Tendance : `{focus_word}`")
        if focus_word.strip():
            df_trend = load_word_trend(focus_word.strip(), start_date, end_date, media_type="tv")
            if df_trend.empty:
                st.info("Ce mot-clé n'apparaît pas sur la période sélectionnée.")
            else:
                df_trend["date"] = df_trend["date"].dt.date
                line = (
                    alt.Chart(df_trend)
                    .mark_line(point=True, strokeWidth=2)
                    .encode(
                        x=alt.X("date:T", title="Date"),
                        y=alt.Y("total_mentions:Q", title="Mentions"),
                        color=alt.Color("source:N", title="Chaîne"),
                        tooltip=["date", "source", "total_mentions"],
                    )
                    .properties(height=300)
                )
                st.altair_chart(line, use_container_width=True)
                with st.expander("Voir le tableau"):
                    st.dataframe(
                        df_trend[["date", "source", "total_mentions"]],
                        use_container_width=True,
                        hide_index=True,
                    )
        else:
            st.info("Saisissez un mot-clé ci-dessus pour voir sa tendance.")

    with col2:
        st.subheader(f"Sujets détaillés TV — {selected_date_topics}")
        df_topics = load_topics_for_day(selected_date_topics, only_tv=True)
        if df_topics.empty:
            st.info("Pas de sujets pour cette date.")
        else:
            df_topics = df_topics.sort_values("articles_count", ascending=False)
            for _, row in df_topics.iterrows():
                with st.expander(
                    f"Topic {int(row['topic_id'])} — {row['topic_label']}  "
                    f"({int(row['articles_count'])} articles)",
                    expanded=False,
                ):
                    kw = row["keywords"]
                    kw_text = ", ".join(kw) if isinstance(kw, (list, tuple)) else str(kw)
                    st.markdown(f"**Mots-clés :** {kw_text}")
