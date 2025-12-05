import os
from datetime import date

import psycopg2
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Charger les variables d'environnement (.env)
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")


@st.cache_resource
def get_connection():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL non défini dans .env")
    return psycopg2.connect(DB_URL)


def load_dates(conn):
    query = """
        SELECT DISTINCT date
        FROM keywords_daily
        ORDER BY date DESC;
    """
    df = pd.read_sql_query(query, conn)
    # La colonne est déjà en datetime.date, pas besoin de .dt.date
    return df["date"].tolist()

    # Convertir explicitement si nécessaire
    #return [d if isinstance(d, date) else pd.to_datetime(d).date() for d in df["date"]]

def load_sources(conn):
    query = """
        SELECT DISTINCT source
        FROM keywords_daily
        ORDER BY source;
    """
    return pd.read_sql_query(query, conn)["source"].tolist()


def load_keywords(conn, selected_date, selected_source):
    query = """
        SELECT word, count, rank, media_type
        FROM keywords_daily
        WHERE date = %s
          AND (%s = 'ALL' OR source = %s)
        ORDER BY rank ASC
        LIMIT 50;
    """
    return pd.read_sql_query(
        query,
        conn,
        params=[selected_date, selected_source, selected_source],
    )


def load_topics(conn, selected_date):
    query = """
        SELECT topic_id, topic_label, articles_count, keywords
        FROM topics_daily
        WHERE date = %s
          AND source = 'ALL'
          AND media_type = 'tv'
        ORDER BY topic_id ASC;
    """
    return pd.read_sql_query(query, conn, params=[selected_date])


def main():
    st.set_page_config(
        page_title="Media Agenda Insights",
        layout="wide"
    )

    st.title("📺 Media Agenda Insights – Dashboard TV")

    conn = get_connection()

    # --- Sidebar : filtres ---
    with st.sidebar:
        st.header("Filtres")

        dates = load_dates(conn)
        if not dates:
            st.error("Aucune date trouvée dans keywords_daily.")
            return

        selected_date = st.date_input(
            "Date",
            value=dates[0],
            min_value=min(dates),
            max_value=max(dates)
        )

        sources = load_sources(conn)
        if "ALL" not in sources:
            sources = ["ALL"] + sources

        selected_source = st.selectbox(
            "Chaîne (source)",
            options=sources,
            index=0
        )

    # --- Colonne 1 : Top mots-clés ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Top mots – {selected_source} – {selected_date}")

        df_kw = load_keywords(conn, selected_date, selected_source)
        if df_kw.empty:
            st.info("Pas de mots-clés pour cette date/source.")
        else:
            st.dataframe(df_kw[["rank", "word", "count", "media_type"]])

            st.bar_chart(
                df_kw.sort_values("rank").set_index("word")["count"],
                use_container_width=True
            )

    # --- Colonne 2 : Top sujets (topics) ---
    with col2:
        st.subheader(f"Sujets (topics) – TV – {selected_date}")

        df_topics = load_topics(conn, selected_date)
        if df_topics.empty:
            st.info("Pas de sujets pour cette date.")
        else:
            for _, row in df_topics.iterrows():
                st.markdown(f"**Topic {int(row['topic_id'])} – {row['topic_label']}**")
                st.write(f"Articles associés : {int(row['articles_count'])}")
                st.write(f"Mots-clés : {', '.join(row['keywords'])}")
                st.markdown("---")


if __name__ == "__main__":
    main()
