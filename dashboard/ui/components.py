from __future__ import annotations
from datetime import date
import streamlit as st
from dashboard.ui.styles import CUSTOM_CSS


def inject_css():
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def section_header(title: str, subtitle: str = ""):
    sub = f'<p style="color:#64748b;margin-top:0.25rem;font-size:0.9rem;">{subtitle}</p>' if subtitle else ""
    st.markdown(
        f"""
        <div style="margin-bottom:1.25rem;">
            <h2 style="color:#f1f5f9;margin:0;font-size:1.4rem;font-weight:600;">{title}</h2>
            {sub}
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi_row(kpis: list[dict]):
    cols = st.columns(len(kpis))
    for col, kpi in zip(cols, kpis):
        with col:
            delta = kpi.get("delta")
            st.metric(
                label=kpi.get("label", ""),
                value=kpi.get("value", "—"),
                delta=delta,
            )


def render_confidence(n: int, min_n: int) -> bool:
    """
    Display a sample-size indicator next to a derived metric.

    Shows "n = X articles" when reliable, or an amber warning badge when
    n < min_n.  Returns True (reliable) or False (low confidence) so
    callers can optionally grey-out or hide the metric.
    """
    if n >= min_n:
        st.caption(f"n = {n:,} articles")
        return True
    st.markdown(
        f'<span style="display:inline-block;'
        f'background:rgba(245,158,11,0.12);color:#f59e0b;'
        f'border:1px solid rgba(245,158,11,0.35);border-radius:4px;'
        f'padding:2px 10px;font-size:0.78rem;">'
        f'⚠ Faible confiance — n = {n:,}'
        f' (seuil : {min_n})</span>',
        unsafe_allow_html=True,
    )
    return False
