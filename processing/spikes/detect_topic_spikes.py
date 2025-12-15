import os
from core.logging import get_logger

import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from core.db_types import PGConnection

load_dotenv()

logger = get_logger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

from core.config import CONFIG

BASELINE_WINDOW = int(CONFIG["spikes"]["baseline_window"])
Z_THRESHOLD = float(CONFIG["spikes"]["z_threshold"])

def get_conn() -> PGConnection:
    return psycopg2.connect(DATABASE_URL)

def load_topic_totals(conn: PGConnection) -> pd.DataFrame:
    sql = """
        SELECT date, topic_id, topic_label,
               SUM(articles_count) AS total_articles
        FROM topics_daily
        WHERE source <> 'ALL'
        GROUP BY date, topic_id, topic_label
        ORDER BY date;
    """
    return pd.read_sql(sql, conn)

def compute_spikes(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["topic_id", "date"])

    results = []

    for topic_id, group in df.groupby("topic_id"):
        g = group.copy().sort_values("date")
        freq = g["total_articles"]

        # rolling baseline using previous N days
        rolling_mean = freq.shift(1).rolling(BASELINE_WINDOW).mean()
        rolling_std = freq.shift(1).rolling(BASELINE_WINDOW).std()

        g["baseline_mean"] = rolling_mean
        g["baseline_std"] = rolling_std
        g["spike_score"] = (freq - rolling_mean) / rolling_std

        g = g[
            (g["baseline_std"] > 0) &
            (g["spike_score"] >= Z_THRESHOLD)
        ]

        results.append(g)

    if not results:
        return pd.DataFrame()

    return pd.concat(results)

def save_spikes(conn: PGConnection, df: pd.DataFrame) -> None:
    sql = """
        INSERT INTO spikes
        (date, topic_id, source, spike_score, baseline_window, details)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    rows = [
        (r.date.date(), r.topic_id, 'ALL',
         float(r.spike_score), BASELINE_WINDOW, None)
        for r in df.itertuples()
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

def main() -> None:
    logger.info("Loading topic totals...")
    with get_conn() as conn:

        try:
            df = load_topic_totals(conn)
            if df.empty:
                logger.info("No topic data found.")
                return

            spike_df = compute_spikes(df)
            logger.info(f"{len(spike_df)} spikes detected.")

            save_spikes(conn, spike_df)
            logger.info("Spike detection COMPLETE.")
        finally:
            pass

if __name__ == "__main__":
    main()
