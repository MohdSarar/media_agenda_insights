# dashboard/views/analytics.py

import streamlit as st
import pandas as pd
import altair as alt

from dashboard.data_access import get_connection
from dashboard.ui.components import section_header, kpi_row


def _load(query: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(query, conn, params=params)


# ─────────────────────────────────────────────────────────────────────────────
# MEDIA BIAS
# ─────────────────────────────────────────────────────────────────────────────

def _render_bias(start_date, end_date):
    df = _load(
        "SELECT date, source, theme, bias_score, methodology "
        "FROM media_bias_scores ORDER BY date DESC, source, theme LIMIT 2000"
    )
    if df.empty:
        st.info("Aucun score de biais disponible pour l'instant.")
        return

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
    if df.empty:
        st.info("Aucune donnée sur la période sélectionnée.")
        return

    sources = sorted(df["source"].unique())
    selected_sources = st.multiselect("Médias à comparer", sources, default=sources, key="bias_sources")
    df = df[df["source"].isin(selected_sources)]
    if df.empty:
        return

    # ── KPIs ──────────────────────────────────────────────────────────────────
    avg_bias = df["bias_score"].mean()
    max_row = df.loc[df["bias_score"].idxmax()]
    min_row = df.loc[df["bias_score"].idxmin()]
    kpi_row([
        {"label": "Score moyen", "value": f"{avg_bias:.2f}"},
        {"label": "Biais max", "value": f"{max_row['bias_score']:.2f}", "delta": f"{max_row['source']} — {max_row['theme']}"},
        {"label": "Biais min", "value": f"{min_row['bias_score']:.2f}", "delta": f"{min_row['source']} — {min_row['theme']}"},
        {"label": "Entrées analysées", "value": f"{len(df):,}"},
    ])

    st.markdown("<div style='margin-top:1.25rem'></div>", unsafe_allow_html=True)

    # ── Heatmap : score moyen par (source × thème) ───────────────────────────
    st.subheader("Heatmap — Score de biais par média et par thème")
    st.caption(
        "Chaque cellule représente le score de biais moyen d'un média sur un thème. "
        "Un score élevé indique une surreprésentation du thème par rapport aux autres médias."
    )
    summary = (
        df.groupby(["source", "theme"])["bias_score"]
        .mean().reset_index()
        .rename(columns={"bias_score": "score_moyen"})
    )
    summary["score_moyen"] = summary["score_moyen"].round(3)

    heatmap = (
        alt.Chart(summary)
        .mark_rect(cornerRadius=2)
        .encode(
            x=alt.X("source:N", title="Média"),
            y=alt.Y("theme:N", title="Thème"),
            color=alt.Color(
                "score_moyen:Q",
                title="Score moyen",
                scale=alt.Scale(scheme="redblue", reverse=True),
            ),
            tooltip=[
                alt.Tooltip("source:N", title="Média"),
                alt.Tooltip("theme:N", title="Thème"),
                alt.Tooltip("score_moyen:Q", title="Score moyen", format=".3f"),
            ],
        )
        .properties(height=320)
    )
    st.altair_chart(heatmap, use_container_width=True)

    # ── Classement des thèmes les plus biaisés ───────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Thèmes les plus biaisés")
        st.caption("Thèmes avec le plus grand écart de couverture entre médias.")
        theme_spread = (
            df.groupby("theme")["bias_score"]
            .agg(score_max="max", score_min="min", score_moyen="mean")
            .assign(ecart=lambda x: x["score_max"] - x["score_min"])
            .reset_index()
            .sort_values("ecart", ascending=False)
        )
        bar = (
            alt.Chart(theme_spread)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
            .encode(
                x=alt.X("ecart:Q", title="Écart max−min"),
                y=alt.Y("theme:N", sort="-x", title=None),
                color=alt.Color("ecart:Q", scale=alt.Scale(scheme="orangered"), legend=None),
                tooltip=[
                    alt.Tooltip("theme:N", title="Thème"),
                    alt.Tooltip("ecart:Q", title="Écart", format=".3f"),
                    alt.Tooltip("score_moyen:Q", title="Moyenne", format=".3f"),
                ],
            )
            .properties(height=280)
        )
        st.altair_chart(bar, use_container_width=True)

    with col2:
        st.subheader("Évolution du biais dans le temps")
        st.caption("Score moyen journalier tous médias confondus.")
        df_time = (
            df.groupby("date")["bias_score"]
            .mean().reset_index()
            .rename(columns={"bias_score": "score_moyen"})
        )
        df_time["date"] = pd.to_datetime(df_time["date"])
        line = (
            alt.Chart(df_time)
            .mark_line(point=True, strokeWidth=2, color="#6366f1")
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("score_moyen:Q", title="Score moyen"),
                tooltip=["date:T", alt.Tooltip("score_moyen:Q", format=".3f")],
            )
            .properties(height=280)
        )
        st.altair_chart(line, use_container_width=True)

    # ── Tableau détaillé ─────────────────────────────────────────────────────
    with st.expander("Voir toutes les données brutes"):
        st.dataframe(df.sort_values(["date", "source", "theme"], ascending=[False, True, True]),
                     use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# TOPIC SPIKES
# ─────────────────────────────────────────────────────────────────────────────

def _render_spikes(start_date, end_date):
    df = _load(
        "SELECT date, topic_id, source, spike_score, baseline_window "
        "FROM spikes ORDER BY date DESC, spike_score DESC LIMIT 1000"
    )
    if df.empty:
        st.info("Aucun spike détecté — il faut au moins quelques semaines d'historique.")
        return

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
    if df.empty:
        st.info("Aucun spike sur la période sélectionnée.")
        return

    kpi_row([
        {"label": "Spikes détectés", "value": len(df)},
        {"label": "Score max", "value": f"{df['spike_score'].max():.2f}"},
        {"label": "Score moyen", "value": f"{df['spike_score'].mean():.2f}"},
        {"label": "Sources touchées", "value": df["source"].nunique()},
    ])

    st.markdown("<div style='margin-top:1.25rem'></div>", unsafe_allow_html=True)
    st.subheader("Top 20 emballements médiatiques")
    st.caption(
        "Un spike correspond à un topic dont la couverture a soudainement explosé "
        "par rapport à sa baseline habituelle. Plus le score est élevé, plus l'emballement est fort."
    )

    top = df.sort_values("spike_score", ascending=False).head(20)
    bar = (
        alt.Chart(top)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("spike_score:Q", title="Score de spike"),
            y=alt.Y("topic_id:N", sort="-x", title="Topic ID"),
            color=alt.Color("source:N", title="Source"),
            tooltip=["date:N", "source:N", "topic_id:N",
                     alt.Tooltip("spike_score:Q", format=".2f"),
                     alt.Tooltip("baseline_window:Q", title="Fenêtre baseline")],
        )
        .properties(height=380)
    )
    st.altair_chart(bar, use_container_width=True)

    with st.expander("Voir toutes les données"):
        st.dataframe(df, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD LIFETIME
# ─────────────────────────────────────────────────────────────────────────────

def _render_keyword_lifetime():
    df = _load(
        "SELECT word, start_date, end_date, duration_days, total_frequency "
        "FROM keyword_lifetime ORDER BY duration_days DESC LIMIT 500"
    )
    if df.empty:
        st.info("Aucune donnée de durée de vie pour les mots-clés.")
        return

    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
    df["end_date"] = pd.to_datetime(df["end_date"]).dt.date

    kpi_row([
        {"label": "Mots-clés suivis", "value": len(df)},
        {"label": "Durée max (jours)", "value": int(df["duration_days"].max())},
        {"label": "Durée moyenne", "value": f"{df['duration_days'].mean():.1f} j"},
        {"label": "Mentions totales (top)", "value": f"{df['total_frequency'].max():,}"},
    ])

    st.markdown("<div style='margin-top:1.25rem'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Mots-clés les plus persistants")
        st.caption("Durée en jours entre première et dernière apparition dans les médias.")
        top_dur = df.sort_values("duration_days", ascending=False).head(20)
        bar = (
            alt.Chart(top_dur)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#6366f1")
            .encode(
                x=alt.X("duration_days:Q", title="Durée (jours)"),
                y=alt.Y("word:N", sort="-x", title=None),
                tooltip=["word:N",
                         alt.Tooltip("duration_days:Q", title="Durée (j)"),
                         "start_date:N", "end_date:N"],
            )
            .properties(height=360)
        )
        st.altair_chart(bar, use_container_width=True)

    with col2:
        st.subheader("Mots-clés les plus mentionnés")
        st.caption("Volume total de mentions cumulées sur toute la période.")
        top_freq = df.sort_values("total_frequency", ascending=False).head(20)
        bar2 = (
            alt.Chart(top_freq)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#8b5cf6")
            .encode(
                x=alt.X("total_frequency:Q", title="Mentions totales"),
                y=alt.Y("word:N", sort="-x", title=None),
                tooltip=["word:N",
                         alt.Tooltip("total_frequency:Q", title="Mentions", format=",")],
            )
            .properties(height=360)
        )
        st.altair_chart(bar2, use_container_width=True)

    with st.expander("Tableau complet"):
        st.dataframe(df, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# TOPIC LIFETIME
# ─────────────────────────────────────────────────────────────────────────────

def _render_topic_lifetime():
    df = _load(
        "SELECT topic_id, topic_label, first_seen_date, last_seen_date, peak_date, total_mentions "
        "FROM topic_lifetime ORDER BY total_mentions DESC LIMIT 500"
    )
    if df.empty:
        st.info("Aucune donnée de durée de vie pour les topics.")
        return

    for col in ["first_seen_date", "last_seen_date", "peak_date"]:
        df[col] = pd.to_datetime(df[col])
    df["duration_days"] = (df["last_seen_date"] - df["first_seen_date"]).dt.days + 1

    kpi_row([
        {"label": "Topics suivis", "value": len(df)},
        {"label": "Durée max (jours)", "value": int(df["duration_days"].max())},
        {"label": "Durée moyenne", "value": f"{df['duration_days'].mean():.1f} j"},
        {"label": "Mentions max", "value": f"{df['total_mentions'].max():,}"},
    ])

    st.markdown("<div style='margin-top:1.25rem'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Topics les plus persistants")
        st.caption("Durée entre première et dernière apparition dans les médias.")
        top_dur = df.sort_values("duration_days", ascending=False).head(15)
        bar = (
            alt.Chart(top_dur)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#6366f1")
            .encode(
                x=alt.X("duration_days:Q", title="Durée (jours)"),
                y=alt.Y("topic_label:N", sort="-x", title=None),
                tooltip=["topic_label:N",
                         alt.Tooltip("duration_days:Q", title="Durée (j)"),
                         alt.Tooltip("first_seen_date:T", title="Première"),
                         alt.Tooltip("last_seen_date:T", title="Dernière"),
                         alt.Tooltip("peak_date:T", title="Pic")],
            )
            .properties(height=340)
        )
        st.altair_chart(bar, use_container_width=True)

    with col2:
        st.subheader("Topics les plus couverts")
        st.caption("Volume total de mentions cumulées.")
        top_cov = df.sort_values("total_mentions", ascending=False).head(15)
        bar2 = (
            alt.Chart(top_cov)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#8b5cf6")
            .encode(
                x=alt.X("total_mentions:Q", title="Mentions totales"),
                y=alt.Y("topic_label:N", sort="-x", title=None),
                tooltip=["topic_label:N",
                         alt.Tooltip("total_mentions:Q", title="Mentions", format=","),
                         alt.Tooltip("peak_date:T", title="Pic médiatique")],
            )
            .properties(height=340)
        )
        st.altair_chart(bar2, use_container_width=True)

    with st.expander("Tableau complet"):
        st.dataframe(
            df[["topic_label", "duration_days", "total_mentions",
                "first_seen_date", "peak_date", "last_seen_date"]],
            use_container_width=True, hide_index=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# THEME LIFETIME
# ─────────────────────────────────────────────────────────────────────────────

def _render_theme_lifetime():
    df = _load(
        "SELECT theme, start_date, end_date, peak_date, total_mentions "
        "FROM theme_lifetime ORDER BY total_mentions DESC LIMIT 200"
    )
    if df.empty:
        st.info("Aucune donnée de durée de vie pour les thèmes.")
        return

    for col in ["start_date", "end_date", "peak_date"]:
        df[col] = pd.to_datetime(df[col])
    df["duration_days"] = (df["end_date"] - df["start_date"]).dt.days + 1

    kpi_row([
        {"label": "Grands thèmes", "value": len(df)},
        {"label": "Thème le + long", "value": df.sort_values("duration_days").iloc[-1]["theme"][:20]},
        {"label": "Durée max (jours)", "value": int(df["duration_days"].max())},
        {"label": "Mentions max", "value": f"{df['total_mentions'].max():,}"},
    ])

    st.markdown("<div style='margin-top:1.25rem'></div>", unsafe_allow_html=True)

    st.subheader("Vue d'ensemble des grands thèmes")
    st.caption(
        "Chaque thème narratif est représenté par sa durée de présence médiatique "
        "et son volume de mentions. Le point rouge indique la date de pic."
    )

    scatter = (
        alt.Chart(df)
        .mark_circle(size=120)
        .encode(
            x=alt.X("duration_days:Q", title="Durée de présence (jours)"),
            y=alt.Y("total_mentions:Q", title="Mentions totales"),
            color=alt.Color("theme:N", title="Thème", legend=None),
            size=alt.Size("total_mentions:Q", legend=None),
            tooltip=[
                "theme:N",
                alt.Tooltip("duration_days:Q", title="Durée (j)"),
                alt.Tooltip("total_mentions:Q", title="Mentions", format=","),
                alt.Tooltip("start_date:T", title="Début"),
                alt.Tooltip("peak_date:T", title="Pic"),
                alt.Tooltip("end_date:T", title="Fin"),
            ],
        )
        .properties(height=360)
    )
    st.altair_chart(scatter, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Plus persistants")
        top_dur = df.sort_values("duration_days", ascending=False).head(10)
        bar = (
            alt.Chart(top_dur)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#6366f1")
            .encode(
                x=alt.X("duration_days:Q", title="Durée (jours)"),
                y=alt.Y("theme:N", sort="-x", title=None),
                tooltip=["theme:N", "duration_days:Q"],
            )
            .properties(height=260)
        )
        st.altair_chart(bar, use_container_width=True)

    with col2:
        st.subheader("Plus couverts")
        top_cov = df.sort_values("total_mentions", ascending=False).head(10)
        bar2 = (
            alt.Chart(top_cov)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#8b5cf6")
            .encode(
                x=alt.X("total_mentions:Q", title="Mentions"),
                y=alt.Y("theme:N", sort="-x", title=None),
                tooltip=["theme:N", alt.Tooltip("total_mentions:Q", format=",")],
            )
            .properties(height=260)
        )
        st.altair_chart(bar2, use_container_width=True)

    with st.expander("Tableau complet"):
        st.dataframe(
            df[["theme", "duration_days", "total_mentions", "start_date", "peak_date", "end_date"]],
            use_container_width=True, hide_index=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN RENDER
# ─────────────────────────────────────────────────────────────────────────────

def render(filters: dict):
    start_date = filters["start_date"]
    end_date = filters["end_date"]

    section_header(
        "Analytics & Biais médiatiques",
        "Analyses avancées : biais éditoriaux, emballements médiatiques, durée de vie des sujets",
    )

    tabs = st.tabs([
        "⚖️ Media Bias",
        "📈 Topic Spikes",
        "🔤 Keyword Lifetime",
        "🧠 Topic Lifetime",
        "🎭 Theme Lifetime",
    ])

    with tabs[0]:
        _render_bias(start_date, end_date)
    with tabs[1]:
        _render_spikes(start_date, end_date)
    with tabs[2]:
        _render_keyword_lifetime()
    with tabs[3]:
        _render_topic_lifetime()
    with tabs[4]:
        _render_theme_lifetime()
