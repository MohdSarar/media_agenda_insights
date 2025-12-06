import os
import logging
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [THEME-LIFETIME] %(message)s")

DATABASE_URL = os.getenv("DATABASE_URL")
GAP_THRESHOLD = 2

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def load_themes(conn):
    sql = """
        SELECT date, topic_label, SUM(articles_count) AS freq
        FROM topics_daily
        WHERE source <> 'ALL' AND topic_label IS NOT NULL
        GROUP BY date, topic_label
        ORDER BY date;
    """
    return pd.read_sql(sql, conn)

def compute_lifetime(df):
    df["date"] = pd.to_datetime(df["date"])
    episodes = []

    for label, group in df.groupby("topic_label"):
        g = group.sort_values("date")

        all_days = pd.date_range(g["date"].min(), g["date"].max(), freq="D")
        merged = pd.DataFrame({"date": all_days})
        merged = merged.merge(g, on="date", how="left").fillna({"freq": 0})

        start = None
        last_date = None
        total = 0
        peak_date = None
        peak_value = 0

        for r in merged.itertuples():
            if start is None:
                if r.freq > 0:
                    start = r.date
                    total = r.freq
                    peak_date = r.date
                    peak_value = r.freq
                last_date = r.date
                continue

            gap = (r.date - last_date).days

            if gap <= GAP_THRESHOLD:
                total += r.freq
                if r.freq > peak_value:
                    peak_value = r.freq
                    peak_date = r.date
            else:
                episodes.append((
                    label, start.date(), last_date.date(),
                    peak_date.date(), int(total)
                ))
                start = r.date if r.freq > 0 else None
                total = r.freq
                peak_date = r.date
                peak_value = r.freq

            last_date = r.date

        if start is not None:
            episodes.append((
                label, start.date(), last_date.date(),
                peak_date.date(), int(total)
            ))

    return episodes

def save(conn, rows):
    sql = """
        CREATE TABLE IF NOT EXISTS theme_lifetime (
            theme TEXT,
            start_date DATE,
            end_date DATE,
            peak_date DATE,
            total_mentions INTEGER
        );
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        execute_values(
            cur,
            "INSERT INTO theme_lifetime VALUES %s",
            rows
        )
    conn.commit()

def main():
    conn = get_conn()
    try:
        df = load_themes(conn)
        rows = compute_lifetime(df)
        save(conn, rows)
        logging.info("Theme lifetime COMPLETE.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
