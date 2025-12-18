# dashboard/views/compare.py

from datetime import date as date_type

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dashboard.data_access import (
    get_available_dates,
    load_topics_timeseries,
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

    if start_date > end_date:
        st.warning("⚠️ Start date is after end date.")
        return

    # Header
    st.markdown(f"""
    <div style="margin-bottom: 1.5rem;">
        <h2 style="color: #f1f5f9; margin: 0; font-size: 1.5rem;">
            📊 Channel Comparison
        </h2>
        <p style="color: #64748b; margin-top: 0.25rem;">
            Comparing media coverage from <strong style="color: #a5b4fc;">{start_date}</strong> to <strong style="color: #a5b4fc;">{end_date}</strong>
        </p>
    </div>
    """, unsafe_allow_html=True)

    df_ts = load_topics_timeseries(start_date, end_date, media_type="tv")
    if df_ts.empty:
        st.info("📭 No topic data for this period.")
        return

    # Normalize source names
    df_ts["source"] = df_ts["source"].fillna("Unknown")
    df_ts["date"] = pd.to_datetime(df_ts["date"], errors="coerce")
    df_ts = df_ts.dropna(subset=["date"])

    # KPI Summary
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    
    with kpi1:
        st.metric(
            label="📡 Sources",
            value=df_ts["source"].nunique()
        )
    
    with kpi2:
        st.metric(
            label="📅 Days Analyzed",
            value=df_ts["date"].nunique()
        )
    
    with kpi3:
        st.metric(
            label="📰 Total Articles",
            value=f"{int(df_ts['total_articles'].sum()):,}"
        )
    
    with kpi4:
        top_source = df_ts.groupby("source")["total_articles"].sum().idxmax()
        st.metric(
            label="🏆 Top Source",
            value=top_source[:12] + "..." if len(top_source) > 12 else top_source
        )

    st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)

    # --- Heatmap : intensité des sujets par date/source ---
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #1e293b 0%, rgba(99, 102, 241, 0.1) 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 1rem 1.25rem;
        margin-bottom: 1rem;
    ">
        <h3 style="color: #f1f5f9; margin: 0; font-size: 1.1rem;">
            🗓️ Coverage Heatmap — Articles by Channel & Day
        </h3>
    </div>
    """, unsafe_allow_html=True)

    df_heat = df_ts.copy()
    df_heat["date_str"] = df_heat["date"].dt.strftime("%Y-%m-%d")

    # Pivot for heatmap
    pivot = df_heat.pivot_table(
        index="source",
        columns="date_str",
        values="total_articles",
        aggfunc="sum",
        fill_value=0
    )

    fig_heat = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale=[
            [0, "#1e293b"],
            [0.5, "#6366f1"],
            [1, "#c084fc"]
        ],
        hoverongaps=False,
        hovertemplate="<b>%{y}</b><br>Date: %{x}<br>Articles: %{z}<extra></extra>"
    ))

    fig_heat.update_layout(
        **PLOTLY_TEMPLATE["layout"],
        height=350,
        margin=dict(l=0, r=0, t=20, b=0),
    )
    fig_heat.update_xaxes(
        tickangle=45,
        tickfont=dict(size=10),
    )

    st.plotly_chart(fig_heat, use_container_width=True)

    # --- Line chart : évolution temporelle ---
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #1e293b 0%, rgba(139, 92, 246, 0.1) 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 1rem 1.25rem;
        margin-bottom: 1rem;
    ">
        <h3 style="color: #f1f5f9; margin: 0; font-size: 1.1rem;">
            📈 Timeline — Articles per Channel
        </h3>
    </div>
    """, unsafe_allow_html=True)

    fig_line = px.line(
        df_ts,
        x="date",
        y="total_articles",
        color="source",
        markers=True,
        color_discrete_sequence=CHART_COLORS,
        labels={"total_articles": "Articles", "date": "Date", "source": "Source"}
    )

    fig_line.update_layout(
        **PLOTLY_TEMPLATE["layout"],
        height=350,
        margin=dict(l=0, r=0, t=20, b=0),
        hovermode="x unified"
    )
    fig_line.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5,
        )
    )

    fig_line.update_traces(
        line=dict(width=2.5),
        marker=dict(size=6),
        hovertemplate="%{y} articles<extra></extra>"
    )

    st.plotly_chart(fig_line, use_container_width=True)

    # --- Bar chart : total par source ---
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #1e293b 0%, rgba(16, 185, 129, 0.1) 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 1rem 1.25rem;
        margin-bottom: 1rem;
    ">
        <h3 style="color: #f1f5f9; margin: 0; font-size: 1.1rem;">
            🏆 Total Coverage by Channel
        </h3>
    </div>
    """, unsafe_allow_html=True)

    df_totals = df_ts.groupby("source")["total_articles"].sum().reset_index()
    df_totals = df_totals.sort_values("total_articles", ascending=True)

    fig_bar = px.bar(
        df_totals,
        x="total_articles",
        y="source",
        orientation="h",
        color="total_articles",
        color_continuous_scale=["#334155", "#6366f1", "#a855f7"],
        labels={"total_articles": "Total Articles", "source": ""}
    )

    fig_bar.update_layout(
        **PLOTLY_TEMPLATE["layout"],
        height=300,
        margin=dict(l=0, r=0, t=20, b=0),
        showlegend=False,
        coloraxis_showscale=False,
    )

    fig_bar.update_traces(
        marker_line_width=0,
        hovertemplate="<b>%{y}</b><br>Total: %{x:,} articles<extra></extra>"
    )

    st.plotly_chart(fig_bar, use_container_width=True)
