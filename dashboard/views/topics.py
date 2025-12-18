# dashboard/views/topics.py

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.data_access import load_word_trend, load_topics_for_day


def render(filters: dict):
    st.subheader("🧠 Topics & tendances")

    start_date = filters["start_date"]
    end_date = filters["end_date"]
    media_type = filters.get("media_type") or "tv"
    source = filters.get("source", "ALL")

    with st.expander("Options (facultatif)", expanded=False):
        focus_word = st.text_input(
            "Mot-clé à suivre (optionnel)",
            value="",
            placeholder="ex: budget, énergie, immigration…",
        ).strip()

    col1, col2 = st.columns([1.15, 1.0], gap="large")

    # --- Left: focus word trend (if provided) ---
    with col1:
        st.markdown("### 🔎 Tendance d'un mot-clé")
        if not focus_word:
            st.info("Renseigne un mot-clé dans 'Options' pour afficher sa tendance.")
        else:
            df = load_word_trend(
                word=focus_word,
                start_date=start_date,
                end_date=end_date,
                media_type=media_type,
            )
            if df.empty:
                st.info("Aucune occurrence trouvée sur la période.")
            else:
                if source != "ALL":
                    df = df[df["source"] == source]

                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df = df.dropna(subset=["date"])

                chart = (
                    alt.Chart(df)
                    .mark_line(point=False)
                    .encode(
                        x=alt.X("date:T", title=None),
                        y=alt.Y("total_mentions:Q", title="Mentions"),
                        color=alt.Color("source:N", legend=alt.Legend(title="Source")),
                        tooltip=["date:T", "source:N", "total_mentions:Q"],
                    )
                    .properties(height=340)
                )
                st.altair_chart(chart, width="stretch")

                with st.expander("Détails (table)", expanded=False):
                    st.dataframe(df.sort_values(["date", "source"]), width="stretch", hide_index=True)

    # --- Right: topics of end_date (TV-focused) ---
    with col2:
        st.markdown(f"### 🗓️ Topics du jour (TV) — {end_date}")
        df_topics = load_topics_for_day(end_date, only_tv=True)
        if df_topics.empty:
            st.info("Pas de topics pour cette date.")
        else:
            df_topics = df_topics.sort_values("articles_count", ascending=False).head(15)
            for _, row in df_topics.iterrows():
                title = f"Topic {int(row['topic_id'])} — {row['topic_label']} ({int(row['articles_count'])})"
                with st.expander(title, expanded=False):
                    keywords = row.get("keywords")
                    kw_text = ", ".join(keywords) if isinstance(keywords, (list, tuple)) else str(keywords)
                    st.caption("Mots-clés")
                    st.write(kw_text)
