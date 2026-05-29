# dashboard/views/topics.py

from io import StringIO

import streamlit as st
import altair as alt

from dashboard.data_access import (
    load_word_trend,
    load_word_trend_fulltext,
    load_topics_for_day,
    load_topics_range,
)
from dashboard.ui.components import section_header


def render(filters: dict):
    start_date = filters["start_date"]
    end_date = filters["end_date"]

    section_header(
        "Exploration des sujets & narratifs",
        f"Tendances de mots-clés et sujets — du {start_date} au {end_date}",
    )

    col1, col2 = st.columns(2)

    # ── Colonne gauche : tendance d'un mot-clé ───────────────────────────────
    with col1:
        focus_word = st.text_input(
            "Mot-clé à analyser",
            value="sécurité",
            placeholder="ex: islam, gaza, voile, budget…",
            key="topics_focus_word",
        )

        search_mode = st.radio(
            "Mode de recherche",
            ["Mots-clés indexés", "Recherche texte intégral"],
            horizontal=True,
            key="topics_search_mode",
            help=(
                "**Indexés** : cherche dans les mots-clés extraits (top 30/jour). "
                "**Texte intégral** : cherche dans le texte brut de tous les articles "
                "— trouve tous les sujets même non indexés (islam, gaza, voile…)."
            ),
        )

        st.subheader(f"Tendance : `{focus_word}`")

        if focus_word.strip():
            word = focus_word.strip().lower()

            if search_mode == "Mots-clés indexés":
                df_trend = load_word_trend(word, start_date, end_date, media_type="tv")
                source_label = "keywords_daily"
            else:
                df_trend = load_word_trend_fulltext(word, start_date, end_date, media_type="tv")
                source_label = "texte intégral"

            if df_trend.empty:
                if search_mode == "Mots-clés indexés":
                    st.warning(
                        f"**`{word}`** absent des mots-clés indexés pour cette période. "
                        "Essayez le mode **Texte intégral** pour chercher dans tous les articles."
                    )
                else:
                    st.info(f"Aucun article ne mentionne **`{word}`** sur la période sélectionnée.")
            else:
                total = int(df_trend["total_mentions"].sum())
                st.caption(
                    f"Source : {source_label} — "
                    f"{total:,} mentions au total sur {df_trend['date'].nunique()} jours"
                )

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
                csv_trend = StringIO()
                df_trend.to_csv(csv_trend, index=False)
                st.download_button(
                    "⬇️ Exporter tendance (CSV)",
                    data=csv_trend.getvalue(),
                    file_name=f"trend_{word}_{start_date}_{end_date}.csv",
                    mime="text/csv",
                    key="dl_trend",
                )
        else:
            st.info("Saisissez un mot-clé pour voir sa tendance.")

    # ── Colonne droite : sujets (jour ou période) ────────────────────────────
    with col2:
        view_mode = st.radio(
            "Vue des sujets",
            ["Jour spécifique", "Sur la période"],
            horizontal=True,
            key="topics_view_mode",
        )

        if view_mode == "Jour spécifique":
            selected_date = st.date_input(
                "Date",
                value=end_date,
                min_value=filters["db_min"],
                max_value=filters["db_max"],
                key="topics_detail_date",
            )
            st.subheader(f"Sujets TV — {selected_date}")
            df = load_topics_for_day(selected_date, only_tv=True)
            if df.empty:
                st.info("Pas de sujets pour cette date.")
            else:
                df = df.sort_values("articles_count", ascending=False)
                for _, row in df.iterrows():
                    with st.expander(
                        f"**{row['topic_label']}** — {int(row['articles_count'])} articles",
                        expanded=False,
                    ):
                        kw = row["keywords"]
                        kw_text = ", ".join(kw) if isinstance(kw, (list, tuple)) else str(kw)
                        st.markdown(f"**Mots-clés :** {kw_text}")

        else:
            st.subheader(f"Top sujets — {start_date} → {end_date}")
            df = load_topics_range(start_date, end_date, media_type="tv", top_n=20)
            if df.empty:
                st.info("Pas de données topics sur cette période.")
            else:
                chart = (
                    alt.Chart(df)
                    .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                    .encode(
                        x=alt.X("total_articles:Q", title="Articles cumulés"),
                        y=alt.Y("topic_label:N", sort="-x", title=None),
                        color=alt.Color(
                            "days_active:Q",
                            title="Jours actif",
                            scale=alt.Scale(scheme="blues"),
                        ),
                        tooltip=[
                            alt.Tooltip("topic_label:N", title="Sujet"),
                            alt.Tooltip("total_articles:Q", title="Articles"),
                            alt.Tooltip("days_active:Q", title="Jours actif"),
                            alt.Tooltip("first_seen:T", title="Première apparition"),
                            alt.Tooltip("last_seen:T", title="Dernière apparition"),
                        ],
                    )
                    .properties(height=420)
                )
                st.altair_chart(chart, use_container_width=True)
                with st.expander("Voir le tableau"):
                    st.dataframe(df, use_container_width=True, hide_index=True)
                csv_topics = StringIO()
                df.to_csv(csv_topics, index=False)
                st.download_button(
                    "⬇️ Exporter sujets (CSV)",
                    data=csv_topics.getvalue(),
                    file_name=f"topics_{start_date}_{end_date}.csv",
                    mime="text/csv",
                    key="dl_topics_period",
                )
