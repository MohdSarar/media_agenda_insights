# dashboard/views/overview.py

from io import StringIO

import streamlit as st
import pandas as pd
import altair as alt

from dashboard.data_access import get_sources, load_keywords_for_day, load_topics_for_day
from dashboard.ui.components import section_header, kpi_row


def render(filters: dict):
    start_date = filters["start_date"]
    end_date = filters["end_date"]
    selected_date = end_date

    section_header(
        "Vue d'ensemble – Agenda du jour",
        f"Mots-clés et sujets dominants pour le {selected_date}",
    )

    # Source filter — inline, not sidebar
    sources = get_sources()
    c_src, _ = st.columns([2, 5])
    with c_src:
        selected_source = st.selectbox("Chaîne", sources, index=0, key="ov_source")

    # Load data
    df_kw = load_keywords_for_day(selected_date, selected_source, media_type=None)
    df_topics = load_topics_for_day(selected_date, only_tv=True)

    # KPI row
    top_word = df_kw.iloc[0]["word"] if not df_kw.empty else "—"
    total_mentions = int(df_kw["count"].sum()) if not df_kw.empty else 0
    kpi_row([
        {"label": "Mots-clés", "value": len(df_kw) if not df_kw.empty else 0},
        {"label": "Sujets TV", "value": len(df_topics) if not df_topics.empty else 0},
        {"label": "Top mot-clé", "value": top_word[:18] if top_word != "—" else "—"},
        {"label": "Total mentions", "value": f"{total_mentions:,}"},
    ])

    st.markdown("<div style='margin-top:1rem'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Top mots-clés — {selected_source}")
        if df_kw.empty:
            st.info("Pas de mots-clés pour cette date / source.")
        else:
            chart = (
                alt.Chart(df_kw.head(20))
                .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                .encode(
                    x=alt.X("count:Q", title="Occurrences"),
                    y=alt.Y("word:N", sort="-x", title=None),
                    color=alt.Color("source:N", legend=None),
                    tooltip=["word", "count", "source", "media_type"],
                )
                .properties(height=350)
            )
            st.altair_chart(chart, use_container_width=True)
            with st.expander("Voir le tableau"):
                st.dataframe(
                    df_kw[["rank", "word", "count", "source", "media_type"]],
                    use_container_width=True,
                    hide_index=True,
                )
            csv_kw = StringIO()
            df_kw.to_csv(csv_kw, index=False)
            st.download_button(
                "⬇️ Exporter mots-clés (CSV)",
                data=csv_kw.getvalue(),
                file_name=f"keywords_{selected_date}_{selected_source}.csv",
                mime="text/csv",
                key="dl_kw_overview",
            )

    with col2:
        st.subheader(f"Sujets dominants TV — {selected_date}")
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
