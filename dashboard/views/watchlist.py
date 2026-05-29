# dashboard/views/watchlist.py
# Feature 4 — Watchlist persistence + push alerts

from __future__ import annotations
from io import StringIO
from datetime import date, timedelta

import pandas as pd
import altair as alt
import streamlit as st

from dashboard.data_access import (
    load_word_trend,
    load_word_trend_fulltext,
    load_watchlist_terms,
    add_watchlist_term,
    remove_watchlist_term,
    load_alert_history,
)
from dashboard.ui.components import section_header


def _spike_alert(df: pd.DataFrame, word: str, window: int = 7, z_thresh: float = 2.0) -> dict | None:
    if df.empty or len(df) < window + 2:
        return None
    df = df.sort_values("date")
    daily = df.groupby("date")["total_mentions"].sum().reset_index()
    if len(daily) < window + 2:
        return None
    baseline = daily.iloc[:-window]["total_mentions"]
    recent = daily.iloc[-window:]["total_mentions"]
    mu = baseline.mean()
    sigma = baseline.std() or 1.0
    recent_mean = recent.mean()
    z = (recent_mean - mu) / sigma
    if z >= z_thresh:
        return {
            "word": word,
            "z_score": round(z, 2),
            "recent_avg": round(recent_mean, 1),
            "baseline_avg": round(mu, 1),
            "peak_date": daily.loc[daily["total_mentions"].idxmax(), "date"],
        }
    return None


def render(filters: dict) -> None:
    start: date = filters["start_date"]
    end: date = filters["end_date"]
    db_min: date = filters["db_min"]
    db_max: date = filters["db_max"]

    section_header(
        "Watchlist & Alertes",
        "Surveillez vos mots-clés et recevez des alertes push (Telegram) sur les pics",
    )

    # ── Watchlist management (DB-backed) ─────────────────────────────────────
    wl = load_watchlist_terms()

    with st.sidebar:
        st.markdown("---")
        st.markdown("**📋 Ma Watchlist**")
        new_word = st.text_input(
            "Ajouter un mot-clé",
            placeholder="ex: réforme, macron…",
            key="wl_add_input",
        )
        if st.button("➕ Ajouter", key="wl_add_btn"):
            w = new_word.strip().lower()
            if w:
                ok = add_watchlist_term(w)
                if ok:
                    st.success(f"« {w} » ajouté.")
                else:
                    st.info(f"« {w} » est déjà dans la watchlist.")
                st.rerun()

        to_remove = st.multiselect(
            "Supprimer",
            options=wl,
            default=[],
            key="wl_remove",
        )
        if to_remove:
            for w in to_remove:
                remove_watchlist_term(w)
            st.rerun()

    wl = load_watchlist_terms()

    if not wl:
        st.info(
            "Votre watchlist est vide. Ajoutez des mots-clés dans la barre latérale.\n\n"
            "**Mots suggérés** : sécurité, budget, énergie, ukraine, immigration"
        )
        return

    # ── Alert detection controls ──────────────────────────────────────────────
    ctrl1, ctrl2 = st.columns([2, 2])
    with ctrl1:
        media_type = st.selectbox("Type de media", ["tv", "press", "ALL"], key="wl_media")
    with ctrl2:
        spike_window = st.slider("Fenêtre spike (derniers N jours)", 3, 14, 7, key="wl_spike_win")
    mt = None if media_type == "ALL" else media_type

    alerts = []
    trend_data: dict[str, pd.DataFrame] = {}

    with st.spinner("Analyse des mots-clés surveillés…"):
        for word in wl:
            df = load_word_trend(word, db_min, db_max, media_type=mt or "tv")
            if df.empty:
                df = load_word_trend_fulltext(word, db_min, db_max, media_type=mt or "tv")
            trend_data[word] = df
            alert = _spike_alert(df, word, window=spike_window)
            if alert:
                alerts.append(alert)

    # ── Alert panel ───────────────────────────────────────────────────────────
    if alerts:
        st.markdown("### 🚨 Alertes détectées")
        for a in sorted(alerts, key=lambda x: x["z_score"], reverse=True):
            st.markdown(
                f"""
                <div style="background:rgba(239,68,68,0.12);border:1px solid rgba(239,68,68,0.3);
                            border-radius:8px;padding:0.75rem 1rem;margin-bottom:0.5rem;">
                    <span style="color:#ef4444;font-weight:700;font-size:1.05rem;">
                        ⚡ `{a['word']}`
                    </span>
                    &nbsp;&nbsp;
                    <span style="color:#fca5a5;">
                        Z-score : <strong>{a['z_score']}</strong> —
                        Moy. récente : <strong>{a['recent_avg']:.0f}</strong>
                        vs historique : <strong>{a['baseline_avg']:.0f}</strong>
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )
        st.markdown("---")
    else:
        st.success(f"Aucun pic significatif détecté sur les {spike_window} derniers jours.")
        st.markdown("---")

    # ── Push alert info ───────────────────────────────────────────────────────
    with st.expander("📣 Alertes push Telegram", expanded=False):
        st.markdown(
            """
            Pour recevoir des notifications Telegram automatiques sur les pics détectés :

            1. Créez un bot via [@BotFather](https://t.me/botfather) et récupérez le token.
            2. Obtenez votre `CHAT_ID` via [@userinfobot](https://t.me/userinfobot).
            3. Définissez les variables d'environnement :
               ```
               TELEGRAM_BOT_TOKEN=your_token
               TELEGRAM_CHAT_ID=your_chat_id
               ```
            4. Ajoutez ce script au cron ou à `pipeline.sh` :
               ```bash
               python alerts/send_alerts.py --days 7
               ```

            Les alertes sont dédupliquées — un seul envoi par (terme, jour, canal).
            """
        )

    # ── Per-keyword trends grid ───────────────────────────────────────────────
    st.markdown("### Tendances sur la période")

    cols_per_row = 2
    rows_of_words = [wl[i: i + cols_per_row] for i in range(0, len(wl), cols_per_row)]

    for row_words in rows_of_words:
        cols = st.columns(cols_per_row)
        for col, word in zip(cols, row_words):
            with col:
                df = trend_data.get(word, pd.DataFrame())
                is_alert = any(a["word"] == word for a in alerts)
                label_color = "#ef4444" if is_alert else "#6366f1"
                st.markdown(
                    f'<p style="color:{label_color};font-weight:600;margin-bottom:0.2rem;">'
                    f'{"⚡ " if is_alert else "📌 "}{word}</p>',
                    unsafe_allow_html=True,
                )
                if df.empty:
                    st.caption("Aucune donnée")
                else:
                    daily = (
                        df.groupby("date")["total_mentions"]
                        .sum()
                        .reset_index()
                        .sort_values("date")
                    )
                    line = (
                        alt.Chart(daily)
                        .mark_area(
                            line={"color": label_color, "strokeWidth": 2},
                            color=label_color,
                            opacity=0.15,
                        )
                        .encode(
                            x=alt.X("date:T", title=None,
                                    axis=alt.Axis(format="%d %b", labelFontSize=9)),
                            y=alt.Y("total_mentions:Q", title=None),
                            tooltip=["date:T", alt.Tooltip("total_mentions:Q", format=",d")],
                        )
                        .properties(height=120)
                        .configure_view(strokeWidth=0, fill="#0f172a")
                        .configure_axis(labelColor="#94a3b8", gridColor="#1e293b")
                    )
                    st.altair_chart(line, use_container_width=True)
                    total = int(df["total_mentions"].sum())
                    st.caption(f"Total période : {total:,} mentions")

    # ── Alert history ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Historique des alertes envoyées")
    hist_df = load_alert_history()
    if hist_df.empty:
        st.caption(
            "Aucune alerte encore envoyée. "
            "Lancez `python alerts/send_alerts.py` pour scanner les pics."
        )
    else:
        hist_df["sent_at"] = pd.to_datetime(hist_df["sent_at"]).dt.strftime("%d/%m/%Y %H:%M")
        hist_df.columns = ["Terme", "Date alerte", "Z-score", "Canal", "Envoyé le"]
        st.dataframe(hist_df, use_container_width=True, hide_index=True)

    # ── Export ────────────────────────────────────────────────────────────────
    st.markdown("---")
    if any(not v.empty for v in trend_data.values()):
        all_trend = pd.concat(
            [df.assign(keyword=w) for w, df in trend_data.items() if not df.empty],
            ignore_index=True,
        )
        csv_buf = StringIO()
        all_trend.to_csv(csv_buf, index=False)
        st.download_button(
            "⬇️ Exporter watchlist (CSV)",
            data=csv_buf.getvalue(),
            file_name=f"watchlist_{start}_{end}.csv",
            mime="text/csv",
        )
