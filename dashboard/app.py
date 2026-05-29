# dashboard/app.py

import sys
from pathlib import Path
from datetime import date

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboard.ui.components import inject_css
from dashboard.data_access import get_available_dates
from dashboard.views import (
    overview,
    compare,
    topics,
    narratives,
    analytics,
    france24_multilingue,
    social_observable,
    agenda_gap,
    lifecycle,
    ner_dashboard,
    divergence,
    framing,
    lead_lag,
    watchlist,
    stance,
)


def main():
    st.set_page_config(
        page_title="Media Agenda Insights",
        page_icon="📡",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    inject_css()

    # ── Sidebar: branding + single global period filter ──────────────────────
    with st.sidebar:
        st.markdown(
            """
            <div style="text-align:center;padding:1.25rem 0 0.75rem;">
                <div style="font-size:2rem;">📡</div>
                <h1 style="
                    font-size:1.3rem;margin:0.25rem 0 0;
                    background:linear-gradient(135deg,#6366f1 0%,#8b5cf6 100%);
                    -webkit-background-clip:text;-webkit-text-fill-color:transparent;
                ">Media Agenda Insights</h1>
                <p style="color:#64748b;font-size:0.75rem;margin-top:0.25rem;">
                    Intelligence Platform
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.divider()

        dates = get_available_dates()
        if dates:
            db_min, db_max = dates[0], dates[-1]
        else:
            db_min = db_max = date.today()

        # Read date range from URL params (permalink support)
        qp = st.query_params
        try:
            _qs = date.fromisoformat(qp.get("start", ""))
            _qe = date.fromisoformat(qp.get("end", ""))
            _default = (_qs, _qe)
        except (ValueError, TypeError):
            _default = (db_min, db_max)

        st.markdown(
            '<p style="color:#94a3b8;font-size:0.75rem;text-transform:uppercase;'
            'letter-spacing:0.08em;margin-bottom:0.5rem;">Période d\'analyse</p>',
            unsafe_allow_html=True,
        )
        period = st.date_input(
            "Période",
            value=_default,
            min_value=db_min,
            max_value=db_max,
            label_visibility="collapsed",
        )

        if isinstance(period, (tuple, list)) and len(period) == 2:
            start_date, end_date = period
        else:
            start_date, end_date = db_min, db_max

        # Sync to URL so the current view is shareable
        st.query_params["start"] = str(start_date)
        st.query_params["end"] = str(end_date)

        st.divider()
        st.markdown(
            """
            <div style="padding:0.75rem;background:rgba(99,102,241,0.08);
                        border-radius:8px;border:1px solid rgba(99,102,241,0.2);">
                <p style="color:#a5b4fc;font-size:0.78rem;margin:0.2rem 0;">🔄 Ingestion RSS automatique</p>
                <p style="color:#a5b4fc;font-size:0.78rem;margin:0.2rem 0;">🧠 NLP &amp; lemmatisation</p>
                <p style="color:#a5b4fc;font-size:0.78rem;margin:0.2rem 0;">📊 Topic modeling &amp; keywords</p>
                <p style="color:#a5b4fc;font-size:0.78rem;margin:0.2rem 0;">🎯 Détection de biais médias</p>
                <p style="color:#a5b4fc;font-size:0.78rem;margin:0.2rem 0;">☁️ Cloud-ready / Docker</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ── Global filters dict passed to every view ─────────────────────────────
    filters = {
        "start_date": start_date,
        "end_date": end_date,
        "db_min": db_min,
        "db_max": db_max,
    }

    # ── Page header ──────────────────────────────────────────────────────────
    st.markdown(
        f"""
        <div style="margin-bottom:1.5rem;">
            <h1 style="font-size:1.8rem;margin:0;color:#f1f5f9;font-weight:700;">
                Media Agenda Insights
            </h1>
            <p style="color:#64748b;margin-top:0.2rem;font-size:0.9rem;">
                Période analysée : <strong style="color:#94a3b8;">{start_date}</strong>
                → <strong style="color:#94a3b8;">{end_date}</strong>
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    tabs = st.tabs([
        "📺 Vue d'ensemble",
        "📊 Comparaison",
        "🧠 Sujets",
        "🧩 Narratifs & Biais",
        "📈 Analytics",
        "🌍 France 24",
        "💬 Social Media",
        "🔍 Agenda Gap",
        "📅 Story Lifecycle",
        "🧬 Entités (NER)",
        "📐 Divergence",
        "🖼️ Framing",
        "⏱️ Lead/Lag",
        "📋 Watchlist",
        "🎭 Stance entités",
    ])

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
    with tabs[7]:
        agenda_gap.render(filters)
    with tabs[8]:
        lifecycle.render(filters)
    with tabs[9]:
        ner_dashboard.render(filters)
    with tabs[10]:
        divergence.render(filters)
    with tabs[11]:
        framing.render(filters)
    with tabs[12]:
        lead_lag.render(filters)
    with tabs[13]:
        watchlist.render(filters)
    with tabs[14]:
        stance.render(filters)

    st.divider()
    st.markdown(
        '<p style="text-align:center;color:#475569;font-size:0.75rem;">'
        'Media Agenda Insights — Data Engineering &amp; NLP Platform</p>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
