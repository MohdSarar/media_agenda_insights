# dashboard/ui/components.py
from __future__ import annotations

from datetime import date
import streamlit as st

from dashboard.data_access import get_available_dates, get_sources
from dashboard.ui.styles import CUSTOM_CSS


def inject_custom_css():
    """Inject custom CSS into the Streamlit app (safe even before widgets render)."""
    # Initialize session keys used by CSS/theme
    if "ui_theme" not in st.session_state:
        st.session_state["ui_theme"] = "Auto"

    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def _clamp_date(d: date, lo: date, hi: date) -> date:
    return max(lo, min(d, hi))


def render_filter_bar():
    """
    Modern filter bar with glassmorphism effect.
    Returns the global filters dict used by all pages.
    """
    # Ensure theme key exists BEFORE rendering widgets (avoids KeyError)
    if "ui_theme" not in st.session_state:
        st.session_state["ui_theme"] = "Auto"

    dates = get_available_dates()
    if not dates:
        st.error("⚠️ No data available in database.")
        st.stop()

    db_min = min(dates)
    db_max = max(dates)

    today = date.today()
    default_end = _clamp_date(today, db_min, db_max)
    default_start = db_min

    # ---- Filter container header ----
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.8) 0%, rgba(99, 102, 241, 0.1) 100%);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 1rem 1.5rem;
            margin-bottom: 1rem;
        ">
            <p style="color: #94a3b8; font-size: 0.75rem; text-transform: uppercase;
                      letter-spacing: 0.1em; margin-bottom: 0.75rem;">
                🎯 Filters
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Add a right-side column for Theme
    c1, c2, c3, c4 = st.columns([2.4, 1.2, 2.2, 0.8])

    with c1:
        period = st.date_input(
            "📅 Period",
            value=(default_start, default_end),
            min_value=db_min,
            max_value=db_max,
            help="Select date range for analysis",
        )

        if isinstance(period, (tuple, list)) and len(period) == 2:
            start_date, end_date = period
        else:
            start_date, end_date = default_start, default_end

    with c2:
        media_type = st.segmented_control(
            "📺 Media Type",
            options=["all", "tv", "press"],
            default="all",
        )
        mt = None if media_type == "all" else media_type

    with c3:
        sources = get_sources(media_type=mt)
        selected_source = st.selectbox(
            "📡 Source",
            sources,
            index=0,
            help="Filter by specific media source",
        )

    with c4:
        st.selectbox(
            "Theme",
            ["Auto", "Dark", "Light"],
            index=["Auto", "Dark", "Light"].index(st.session_state["ui_theme"]),
            key="ui_theme",
            label_visibility="collapsed",
            help="UI theme (Auto uses Streamlit default)",
        )

    return {
        "start_date": start_date,
        "end_date": end_date,
        "media_type": mt,           # None = all
        "source": selected_source,  # "ALL" or a real source
        "db_min": db_min,
        "db_max": db_max,
        "ui_theme": st.session_state["ui_theme"],
    }


# ---- The rest of your helpers unchanged ----

def render_metric_card(title: str, value: str, delta: str = None, delta_type: str = "positive"):
    delta_html = ""
    if delta:
        delta_icon = "↑" if delta_type == "positive" else "↓"
        delta_html = f'<div class="custom-card-delta {delta_type}">{delta_icon} {delta}</div>'

    st.markdown(
        f"""
        <div class="custom-card">
            <div class="custom-card-title">{title}</div>
            <div class="custom-card-value">{value}</div>
            {delta_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_header(title: str, icon: str = "📊"):
    st.markdown(
        f"""
        <div class="section-header">
            <span class="icon">{icon}</span>
            <h2>{title}</h2>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_badge(text: str, variant: str = "primary"):
    return f'<span class="badge badge-{variant}">{text}</span>'


def render_stat_row(stats: list[dict]):
    cols = st.columns(len(stats))
    for col, stat in zip(cols, stats):
        with col:
            delta = stat.get("delta")
            delta_type = stat.get("delta_type", "positive")

            if delta:
                st.metric(
                    label=stat["title"],
                    value=stat["value"],
                    delta=delta,
                    delta_color="normal" if delta_type == "positive" else "inverse",
                )
            else:
                st.metric(label=stat["title"], value=stat["value"])


def render_kpi_row(kpis: list[dict]):
    cols = st.columns(len(kpis))
    for col, kpi in zip(cols, kpis):
        with col:
            icon = kpi.get("icon", "📊")
            title = kpi.get("title", "")
            value = kpi.get("value", "")
            subtitle = kpi.get("subtitle", "")

            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, #1e293b 0%, rgba(99, 102, 241, 0.1) 100%);
                    border: 1px solid #334155;
                    border-radius: 12px;
                    padding: 1.25rem;
                    text-align: center;
                    transition: all 0.3s ease;
                ">
                    <div style="font-size: 2rem; margin-bottom: 0.5rem;">{icon}</div>
                    <div style="color: #94a3b8; font-size: 0.8rem; text-transform: uppercase;
                                letter-spacing: 0.05em;">{title}</div>
                    <div style="color: #f1f5f9; font-size: 1.75rem; font-weight: 700;
                                margin: 0.25rem 0;">{value}</div>
                    <div style="color: #64748b; font-size: 0.75rem;">{subtitle}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_empty_state(message: str, icon: str = "📭"):
    st.markdown(
        f"""
        <div style="text-align: center; padding: 3rem; color: #64748b;">
            <div style="font-size: 3rem; margin-bottom: 1rem;">{icon}</div>
            <p style="font-size: 1rem;">{message}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
