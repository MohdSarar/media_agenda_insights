# dashboard/app.py

import sys
from pathlib import Path

import streamlit as st

# Add repo root to PYTHONPATH so `import dashboard...` works on Streamlit Cloud
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboard.ui.components import render_filter_bar, inject_custom_css
from dashboard.views import (
    overview,
    compare,
    topics,
    narratives,
    analytics,
    france24_multilingue,
    social_observable,
)


def main():
    st.set_page_config(
        page_title="Media Agenda Insights",
        page_icon="📡",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    inject_custom_css()

    # Sidebar (branding only) — keep minimal
    with st.sidebar:
        st.markdown(
            """
            <div style="text-align: center; padding: 1rem 0;">
                <h1 style="font-size: 1.8rem; margin: 0;
                           background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
                           -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
                    📡 Media Agenda
                </h1>
                <p style="color: #94a3b8; font-size: 0.85rem; margin-top: 0.5rem;">
                    Intelligence Platform
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("---")
        st.markdown(
            """
            <div style="padding: 0.5rem; background: rgba(99, 102, 241, 0.1);
                        border-radius: 8px; margin-bottom: 1rem;">
                <p style="color: #a5b4fc; font-size: 0.8rem; margin: 0.3rem 0;">🔄 Auto RSS Ingestion</p>
                <p style="color: #a5b4fc; font-size: 0.8rem; margin: 0.3rem 0;">🧠 NLP Processing</p>
                <p style="color: #a5b4fc; font-size: 0.8rem; margin: 0.3rem 0;">📊 Topic Modeling & Keywords</p>
                <p style="color: #a5b4fc; font-size: 0.8rem; margin: 0.3rem 0;">🎯 Media Bias Detection</p>
                <p style="color: #a5b4fc; font-size: 0.8rem; margin: 0.3rem 0;">☁️ Cloud-Ready Architecture</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Header
    st.markdown(
        """
        <div style="margin-bottom: 1.5rem;">
            <h1 style="font-size: 2rem; margin: 0; color: #f1f5f9;">
                Media Agenda Insights
            </h1>
            <p style="color: #64748b; margin-top: 0.25rem;">
                Real-time media monitoring & narrative analysis
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Filters bar (global)
    filters = render_filter_bar()

    st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)

    tabs = st.tabs(
        [
            "📺 Overview",
            "📊 Compare",
            "🧠 Topics",
            "🧩 Narratives",
            "📈 Analytics",
            "🌍 France24",
            "💬 Social",
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

    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style="text-align: center; color: #64748b; font-size: 0.75rem; padding: 1rem;">
            Built with ❤️ by <strong>Madel Data</strong> • Media Agenda Insights v2.0
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()

    