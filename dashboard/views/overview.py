# dashboard/views/overview.py

from datetime import date as date_type

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dashboard.data_access import (
    get_available_dates,
    get_sources,
    load_keywords_for_day,
    load_topics_for_day,
)
from dashboard.ui.styles import CHART_COLORS, PLOTLY_TEMPLATE


def render(filters: dict):
    # Global filters
    start_date = filters["start_date"]
    end_date = filters["end_date"]
    selected_source = filters["source"]
    media_type = filters["media_type"]

    # For overview you likely want a single day (end_date)
    selected_date = end_date

    dates = get_available_dates()
    if not dates:
        st.error("⚠️ No data available in keywords_daily.")
        return

    min_date, max_date = dates[0], dates[-1]

    # Header with KPIs
    st.markdown(f"""
    <div style="margin-bottom: 1.5rem;">
        <h2 style="color: #f1f5f9; margin: 0; font-size: 1.5rem;">
            📺 Daily Overview — {selected_date}
        </h2>
        <p style="color: #64748b; margin-top: 0.25rem;">
            Media agenda highlights for {selected_source if selected_source != 'ALL' else 'all sources'}
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Load data
    df_kw = load_keywords_for_day(selected_date, selected_source, media_type=None)
    df_topics = load_topics_for_day(selected_date, only_tv=True)

    # KPI Cards
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    
    with kpi1:
        st.metric(
            label="🔤 Keywords",
            value=len(df_kw) if not df_kw.empty else 0,
            delta="Today"
        )
    
    with kpi2:
        st.metric(
            label="🧠 Topics",
            value=len(df_topics) if not df_topics.empty else 0,
            delta="TV Sources"
        )
    
    with kpi3:
        top_word = df_kw.iloc[0]["word"] if not df_kw.empty else "—"
        st.metric(
            label="🔥 Top Keyword",
            value=top_word[:15] + "..." if len(str(top_word)) > 15 else top_word
        )
    
    with kpi4:
        total_mentions = int(df_kw["count"].sum()) if not df_kw.empty else 0
        st.metric(
            label="📊 Total Mentions",
            value=f"{total_mentions:,}"
        )

    st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    # --- Colonne 1 : Top mots-clés ---
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
                🔤 Top Keywords — {selected_source}
            </h3>
        </div>
        """, unsafe_allow_html=True)

        if df_kw.empty:
            st.info("📭 No keywords for this date/source.")
        else:
            # Top 15 keywords for chart
            df_chart = df_kw.head(15).copy()
            
            # Horizontal bar chart with Plotly
            fig = px.bar(
                df_chart,
                x="count",
                y="word",
                orientation="h",
                color="count",
                color_continuous_scale=["#334155", "#6366f1", "#8b5cf6"],
                labels={"count": "Occurrences", "word": ""},
            )
            
            fig.update_layout(
                **PLOTLY_TEMPLATE["layout"],
                height=400,
                showlegend=False,
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=20, b=0),
            )
            fig.update_yaxes(categoryorder="total ascending")
            
            fig.update_traces(
                marker_line_width=0,
                hovertemplate="<b>%{y}</b><br>Count: %{x}<extra></extra>"
            )
            
            st.plotly_chart(fig, use_container_width=True)

            # Compact table
            with st.expander("📋 View Full Table", expanded=False):
                st.dataframe(
                    df_kw[["rank", "word", "count", "source", "media_type"]],
                    use_container_width=True,
                    hide_index=True,
                )

    # --- Colonne 2 : Sujets du jour ---
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
                🧠 Today's Topics — TV
            </h3>
        </div>
        """, unsafe_allow_html=True)

        if df_topics.empty:
            st.info("📭 No topics for this date.")
        else:
            # Sort by importance
            df_topics = df_topics.sort_values("articles_count", ascending=False)

            # Topic distribution pie chart
            fig_pie = px.pie(
                df_topics.head(8),
                values="articles_count",
                names="topic_label",
                color_discrete_sequence=CHART_COLORS,
                hole=0.4,
            )
            
            fig_pie.update_layout(
                **PLOTLY_TEMPLATE["layout"],
                height=300,
                margin=dict(l=0, r=0, t=20, b=0),
                showlegend=True,
            )
            fig_pie.update_layout(
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.3,
                    xanchor="center",
                    x=0.5,
                    font=dict(size=10)
                )
            )
            
            fig_pie.update_traces(
                textposition="inside",
                textinfo="percent",
                hovertemplate="<b>%{label}</b><br>Articles: %{value}<br>%{percent}<extra></extra>"
            )
            
            st.plotly_chart(fig_pie, use_container_width=True)

            # Topic cards
            st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)
            
            for _, row in df_topics.head(6).iterrows():
                keywords = row["keywords"]
                if isinstance(keywords, (list, tuple)):
                    kw_text = ", ".join(keywords[:5])
                else:
                    kw_text = str(keywords)[:50]
                
                with st.expander(
                    f"📌 Topic {int(row['topic_id'])} — {row['topic_label']} ({int(row['articles_count'])} articles)",
                    expanded=False,
                ):
                    st.markdown(f"""
                    <div style="color: #94a3b8;">
                        <strong style="color: #a5b4fc;">Keywords:</strong> {kw_text}
                    </div>
                    """, unsafe_allow_html=True)
