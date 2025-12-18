# dashboard/ui/kpis.py
import streamlit as st

def kpi_row(items):
    """
    items: list[dict] -> {label, value, delta(optional), help(optional)}
    """
    cols = st.columns(len(items))
    for col, it in zip(cols, items):
        with col:
            st.metric(
                it["label"],
                it["value"],
                delta=it.get("delta"),
                help=it.get("help"),
            )
