# dashboard/views/topics.py

from datetime import date as date_type

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dashboard.data_access import (
    get_available_dates,
    load_word_trend,
    load_topics_for_day,
)
from dashboard.ui.styles import CHART_COLORS, PLOTLY_TEMPLATE


def render(filters: dict):
    start_date = filters["start_date"]
    end_date = filters["end_date"]
    selected_source = filters["source"]
    media_type = filters["media_type"]

    dates = get_available_dates()
    if not dates:
        st.error("⚠️ No data available.")
        return

    min_date, max_date = dates[0], dates[-1]

    # Header
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h2 style="color: #f1f5f9; margin: 0; font-size: 1.5rem;">
            🧠 Topic & Keyword Explorer
        </h2>
        <p style="color: #64748b; margin-top: 0.25rem;">
            Deep dive into topics and track keyword trends over time
        </p>
    </div>
    """, unsafe_allow_html=True)

    kw_col1, kw_col2 = st.columns([1.6, 1])

    with kw_col1:
        keyword = st.text_input(
            "Keyword",
            placeholder="Enter a keyword (e.g. gaza, budget, election...)",
            label_visibility="collapsed",
            key="topics_keyword",
        )

    with kw_col2:
        st.toggle("Exact match", value=False, key="topics_keyword_exact")


    # Sidebar filters
    with st.sidebar:
        st.markdown("""
        <div style="
            background: rgba(99, 102, 241, 0.1);
            border-radius: 8px;
            padding: 1rem;
            margin-bottom: 1rem;
        ">
            <h4 style="color: #a5b4fc; margin: 0 0 0.75rem 0; font-size: 0.9rem;">
                🔍 Keyword Analyzer
            </h4>
        </div>
        """, unsafe_allow_html=True)

        focus_word = st.text_input(
            "Track a keyword",
            value="",
            placeholder="e.g., économie, sécurité, ukraine",
            help="Enter a keyword to see its trend over time"
        )

        st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

        selected_date_topics: date_type = st.date_input(
            "📅 Topic exploration date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key="topics_date",
        )

    col1, col2 = st.columns(2)

    # --- Colonne 1 : tendance d'un mot-clé ---
    with col1:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #1e293b 0%, rgba(99, 102, 241, 0.1) 100%);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 1rem 1.25rem;
            margin-bottom: 1rem;
        ">
            <h3 style="color: #f1f5f9; margin: 0; font-size: 1.1rem;">
                📈 Keyword Trend: <span style="color: #a5b4fc;">{focus_word if focus_word else '—'}</span>
            </h3>
        </div>
        """, unsafe_allow_html=True)

        if focus_word.strip():
            df_trend = load_word_trend(focus_word.strip(), start_date, end_date, media_type="tv")

            if df_trend.empty:
                st.info(f"📭 No mentions of **'{focus_word}'** in the selected period.")
            else:
                df_trend["date"] = pd.to_datetime(df_trend["date"])

                # Area chart for trend
                fig = px.area(
                    df_trend,
                    x="date",
                    y="total_mentions",
                    color="source",
                    color_discrete_sequence=CHART_COLORS,
                    labels={"total_mentions": "Mentions", "date": "Date", "source": "Source"}
                )

                fig.update_layout(
                    **PLOTLY_TEMPLATE["layout"],
                    height=350,
                    margin=dict(l=0, r=0, t=20, b=0),
                    hovermode="x unified"
                )
                fig.update_layout(
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.3,
                        xanchor="center",
                        x=0.5,
                    )
                )

                fig.update_traces(
                    hovertemplate="%{y} mentions<extra></extra>",
                    line=dict(width=2),
                )

                st.plotly_chart(fig, use_container_width=True)

                # Summary stats
                total = int(df_trend["total_mentions"].sum())
                peak_date = df_trend.loc[df_trend["total_mentions"].idxmax(), "date"]
                
                stat1, stat2 = st.columns(2)
                with stat1:
                    st.metric("📊 Total Mentions", f"{total:,}")
                with stat2:
                    st.metric("🔥 Peak Date", peak_date.strftime("%Y-%m-%d"))

                with st.expander("📋 View Data Table", expanded=False):
                    df_display = df_trend.copy()
                    df_display["date"] = df_display["date"].dt.strftime("%Y-%m-%d")
                    st.dataframe(
                        df_display[["date", "source", "total_mentions"]],
                        use_container_width=True,
                        hide_index=True,
                    )
        else:
            st.markdown("""
            <div style="
                text-align: center;
                padding: 3rem;
                color: #64748b;
                background: rgba(30, 41, 59, 0.5);
                border-radius: 12px;
                border: 1px dashed #334155;
            ">
                <div style="font-size: 2.5rem; margin-bottom: 1rem;">🔍</div>
                <p style="margin: 0;">Enter a keyword in the sidebar to track its trend</p>
            </div>
            """, unsafe_allow_html=True)

    # --- Colonne 2 : topics détaillés pour une date ---
    with col2:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #1e293b 0%, rgba(139, 92, 246, 0.1) 100%);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 1rem 1.25rem;
            margin-bottom: 1rem;
        ">
            <h3 style="color: #f1f5f9; margin: 0; font-size: 1.1rem;">
                🧠 Topics for <span style="color: #c4b5fd;">{selected_date_topics}</span>
            </h3>
        </div>
        """, unsafe_allow_html=True)

        df_topics = load_topics_for_day(selected_date_topics, only_tv=True)
        if df_topics.empty:
            st.info("📭 No topics for this date.")
        else:
            df_topics = df_topics.sort_values("articles_count", ascending=False)

            # Treemap visualization
            fig_tree = px.treemap(
                df_topics.head(10),
                path=["topic_label"],
                values="articles_count",
                color="articles_count",
                color_continuous_scale=["#334155", "#6366f1", "#a855f7"],
            )

            fig_tree.update_layout(
                **PLOTLY_TEMPLATE["layout"],
                height=300,
                margin=dict(l=0, r=0, t=20, b=0),
                coloraxis_showscale=False,
            )

            fig_tree.update_traces(
                hovertemplate="<b>%{label}</b><br>Articles: %{value}<extra></extra>",
                textinfo="label+value",
                textfont=dict(size=12, color="white"),
            )

            st.plotly_chart(fig_tree, use_container_width=True)

            # Topic cards
            st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)
            
            for idx, row in df_topics.head(8).iterrows():
                keywords = row["keywords"]
                if isinstance(keywords, (list, tuple)):
                    kw_text = ", ".join(keywords[:6])
                else:
                    kw_text = str(keywords)[:60]

                # Color based on ranking
                colors = ["#6366f1", "#8b5cf6", "#a855f7", "#c084fc", "#d8b4fe"]
                color = colors[min(idx, len(colors)-1)] if isinstance(idx, int) else colors[0]
                
                with st.expander(
                    f"📌 {row['topic_label']} — {int(row['articles_count'])} articles",
                    expanded=False,
                ):
                    st.markdown(f"""
                    <div style="
                        background: rgba(99, 102, 241, 0.05);
                        border-left: 3px solid {color};
                        padding: 0.75rem;
                        border-radius: 0 8px 8px 0;
                    ">
                        <p style="color: #94a3b8; margin: 0; font-size: 0.85rem;">
                            <strong style="color: #a5b4fc;">Keywords:</strong> {kw_text}
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
