# dashboard/views/digest.py
# Feature 3 — Weekly Auto-generated Digest

from __future__ import annotations
import json
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from dashboard.data_access import load_weekly_digests
from dashboard.ui.components import section_header


def render(filters: dict) -> None:
    section_header(
        "Digest hebdomadaire",
        "Résumé éditorial IA des tendances médias — généré automatiquement chaque semaine",
    )

    st.markdown(
        """
        Le **digest** est généré par Claude Haiku à partir des topics dominants,
        mots-clés et personnalités de la semaine. Il offre une lecture éditoriale
        en 5 points des grandes tendances de l'agenda médiatique français.

        > **Générer un digest** :
        > `python processing/digest/generate_weekly_digest.py [--week YYYY-MM-DD] [--dry-run]`
        """
    )

    with st.spinner("Chargement des digests…"):
        df = load_weekly_digests(limit=12)

    if df.empty:
        st.info(
            "Aucun digest disponible. "
            "Lancez : `python processing/digest/generate_weekly_digest.py`"
        )
        return

    # ── Week selector ─────────────────────────────────────────────────────────
    df["week_label"] = df.apply(
        lambda r: f"Semaine du {r['week_start']} au {r['week_end']}", axis=1
    )
    selected_label = st.selectbox(
        "Semaine",
        options=df["week_label"].tolist(),
        key="digest_week_select",
    )

    row = df[df["week_label"] == selected_label].iloc[0]

    # ── Digest text ───────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(
        f'<p style="color:#64748b;font-size:0.8rem;margin-bottom:0.5rem;">'
        f'Généré le {pd.to_datetime(row["generated_at"]).strftime("%d/%m/%Y %H:%M")}'
        f'</p>',
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div style="
            background: rgba(99,102,241,0.06);
            border: 1px solid rgba(99,102,241,0.2);
            border-radius: 10px;
            padding: 1.25rem 1.5rem;
            font-size: 0.95rem;
            line-height: 1.7;
            color: #e2e8f0;
        ">
        {row["digest_text"].replace(chr(10), "<br>")}
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Context data ──────────────────────────────────────────────────────────
    with st.expander("📊 Données sources utilisées pour ce digest", expanded=False):
        ctx = row["context_json"]
        if isinstance(ctx, str):
            try:
                ctx = json.loads(ctx)
            except Exception:
                ctx = {}

        if isinstance(ctx, dict):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Top sujets**")
                if ctx.get("top_topics"):
                    topics_df = pd.DataFrame(ctx["top_topics"])
                    st.dataframe(topics_df, use_container_width=True, hide_index=True)
                else:
                    st.caption("(aucun)")

                st.markdown("**Top mots-clés**")
                st.write(", ".join(ctx.get("top_keywords", [])) or "(aucun)")

            with col2:
                st.markdown("**Personnalités mentionnées**")
                st.write(", ".join(ctx.get("top_persons", [])) or "(aucun)")

                st.markdown("**Volume par chaîne**")
                sc = ctx.get("source_counts", {})
                if sc:
                    sc_df = pd.DataFrame(
                        [{"Chaîne": k, "Articles": v} for k, v in sc.items()]
                    )
                    st.dataframe(sc_df, use_container_width=True, hide_index=True)

    # ── Archive navigation ─────────────────────────────────────────────────────
    if len(df) > 1:
        st.markdown("---")
        st.markdown("#### Archive des digests")
        archive = df[["week_label", "generated_at"]].copy()
        archive["generated_at"] = pd.to_datetime(archive["generated_at"]).dt.strftime(
            "%d/%m/%Y %H:%M"
        )
        archive.columns = ["Semaine", "Généré le"]
        st.dataframe(archive, use_container_width=True, hide_index=True)
