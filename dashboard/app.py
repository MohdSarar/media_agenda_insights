# dashboard/app.py

import sys
from pathlib import Path
from datetime import date as date_type

# Add repo root to PYTHONPATH so `import dashboard...` works on Streamlit Cloud
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st

from dashboard.data_access import get_available_dates, get_sources
from dashboard.views import (
    overview,
    compare,
    topics,
    narratives,
    analytics,
    france24_multilingue,
    social_observable,
)


def _clamp(d: date_type, lo: date_type, hi: date_type) -> date_type:
    return max(lo, min(d, hi))


def _render_global_filter_bar() -> dict:
    """Single source of truth for filters (no sidebar, no per-tab date inputs)."""
    dates = get_available_dates()
    if not dates:
        st.error(
            "Aucune donnée disponible. Vérifie que la base contient au moins "
            "keywords_daily/topics_daily/articles_raw."
        )
        st.stop()

    db_min, db_max = min(dates), max(dates)

    today = date_type.today()
    default_start = db_min
    default_end = _clamp(today, db_min, db_max)  # avoid future dates (empty)

    # Compact look & feel
    st.markdown(
        """
        <style>
          div[data-testid="stMetric"] { padding: 0.35rem 0.5rem; border-radius: 14px; border: 1px solid rgba(49,51,63,0.15); }
          .mai-filterbar { padding: 0.75rem 0.75rem; border-radius: 18px; border: 1px solid rgba(49,51,63,0.15); background: rgba(255,255,255,0.35); }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.container(border=False):
        st.markdown('<div class="mai-filterbar">', unsafe_allow_html=True)
        c1, c2, c3 = st.columns([2.2, 1.1, 1.2], vertical_alignment="bottom")

        with c1:
            period = st.date_input(
                "Période",
                value=(default_start, default_end),
                min_value=db_min,
                max_value=db_max,
                key="global_period",
            )
            if isinstance(period, (tuple, list)) and len(period) == 2:
                start_date, end_date = period
            else:
                start_date, end_date = default_start, default_end

        with c2:
            media_choice = st.segmented_control(
                "Média",
                options=["all", "tv", "press"],
                default="all",
                key="global_media",
            )
            media_type = None if media_choice == "all" else media_choice

        with c3:
            with st.popover("Filtres avancés", use_container_width=True):
                sources = get_sources(media_type=media_type)
                source = st.selectbox("Source", sources, index=0, key="global_source")
                st.caption("Astuce: laisse **ALL** pour voir l'ensemble.")
            source = st.session_state.get("global_source", "ALL")

        st.markdown("</div>", unsafe_allow_html=True)

    with st.expander("À propos", expanded=False):
        st.markdown(
            """
            **Media Agenda Insights** — Dashboard d'analyse de l'agenda médiatique.

            - Ingestion RSS TV + presse, pipeline NLP, extraction mots-clés & topics
            - France 24 multilingue isolé dans des tables dédiées
            - Observatoire social: posts/mentions par plateforme, source, langue
            """
        )

    return {
        "start_date": start_date,
        "end_date": end_date,
        "media_type": media_type,  # None = all
        "source": source,          # "ALL" or concrete
        "db_min": db_min,
        "db_max": db_max,
    }


def main():
    st.set_page_config(
        page_title="Media Agenda Insights",
        page_icon="🛰️",
        layout="wide",
    )

    st.title("🛰️ Media Agenda Insights")
    filters = _render_global_filter_bar()

    tabs = st.tabs(
        [
            "Overview",
            "Compare",
            "Topics",
            "Narratives",
            "Analytics",
            "France 24",
            "Social",
        ]
    )

    with tabs[0]:
        overview.render(filters)

    with tabs[1]:
        compare.render(filters)

    with tabs[2]:
        topics.render(filters)

    with tabs[3]:
        narratives.render(filters)

    with tabs[4]:
        analytics.render(filters)

    with tabs[5]:
        france24_multilingue.render(filters)

    with tabs[6]:
        social_observable.render(filters)


if __name__ == "__main__":
    main()
