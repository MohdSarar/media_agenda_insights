# dashboard/ui/components.py
from __future__ import annotations

from datetime import date
import streamlit as st

from dashboard.data_access import get_available_dates, get_sources


def _clamp_date(d: date, lo: date, hi: date) -> date:
    return max(lo, min(d, hi))


def render_filter_bar():
    """
    Compact top filter bar (minimal widgets) with smart defaults:
    - default range: oldest DB date -> today (clamped to DB max)
    - media_type: all/tv/press
    - source: list depends on media_type
    """
    dates = get_available_dates()
    if not dates:
        st.error("Aucune date disponible dans la base (keywords_daily/topics_daily/articles_raw).")
        st.stop()

    db_min = min(dates)
    db_max = max(dates)

    today = date.today()
    default_end = _clamp_date(today, db_min, db_max)
    default_start = db_min

    c1, c2, c3 = st.columns([2.2, 1.0, 2.2], vertical_alignment="bottom")

    with c1:
        period = st.date_input(
            "Période",
            value=(default_start, default_end),
            min_value=db_min,
            max_value=db_max,
        )

        if isinstance(period, (tuple, list)) and len(period) == 2:
            start_date, end_date = period
        else:
            start_date, end_date = default_start, default_end

    with c2:
        media_type = st.segmented_control(
            "Média",
            options=["all", "tv", "press"],
            default="all",
        )
        mt = None if media_type == "all" else media_type

    with c3:
        sources = get_sources(media_type=mt)
        selected_source = st.selectbox("Source", sources, index=0)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "media_type": mt,          # None = all
        "source": selected_source, # "ALL" or a real source
        "db_min": db_min,
        "db_max": db_max,
    }
