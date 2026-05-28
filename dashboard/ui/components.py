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
