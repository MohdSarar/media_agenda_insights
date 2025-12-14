import os
import logging
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from core.db_types import PGConnection

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://media_user:media_pass@localhost:5432/media_agenda_insights"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BIAS] %(message)s")

def get_conn() -> PGConnection:
    return psycopg2.connect(DATABASE_URL)

def load_topics(conn: PGConnection) -> pd.DataFrame:
    sql = """
        SELECT date, source, topic_id, topic_label, articles_count
        FROM topics_daily
        WHERE source <> 'ALL'
        ORDER BY date, topic_id, source;
    """
    return pd.read_sql(sql, conn)

def compute_bias(df: pd.DataFrame) -> pd.DataFrame:
    """
    share = count_source / total_count
    expected_share = 1 / number_of_sources
    bias_score = share - expected_share
    """
    if df.empty:
        return pd.DataFrame()

    agg = (
        df.groupby(["date", "topic_id"], as_index=False)
        .agg(total_articles=("articles_count", "sum"),
             n_sources=("source", "nunique"))
    )

    df = df.merge(agg, on=["date", "topic_id"], how="left")

    df["share"] = df["articles_count"] / df["total_articles"]
    df["expected_share"] = 1.0 / df["n_sources"]
    df["bias_score"] = df["share"] - df["expected_share"]

    out = df[[
        "date", "source", "topic_label",
        "bias_score", "share", "expected_share", "articles_count"
    ]].copy()

    out["methodology"] = "topic-level share vs expected uniform distribution"
    out["details"] = None

    return out

def save_bias(conn: PGConnection, df: pd.DataFrame) -> None:
    if df.empty:
        logging.info("No bias results to save.")
        return

    sql = """
        INSERT INTO media_bias_scores
        (date, source, theme, bias_score, methodology, details)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    rows = [
        (r.date, r.source, r.topic_label, float(r.bias_score),
         r.methodology, None)
        for r in df.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

def main() -> None:
    logging.info("Loading topic data...")
    conn = get_conn()
    try:
        df = load_topics(conn)
        logging.info(f"{len(df)} topic rows loaded.")

        bias_df = compute_bias(df)
        logging.info(f"{len(bias_df)} bias rows computed.")

        save_bias(conn, bias_df)
        logging.info("Bias analysis COMPLETE.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
