from __future__ import annotations
import os
from core.logging import get_logger
from typing import Any, Optional
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from datetime import timedelta
from dotenv import load_dotenv
from core.db_types import PGConnection

load_dotenv()
logger = get_logger(__name__)
DATABASE_URL = os.getenv("DATABASE_URL")
GAP_THRESHOLD = 2   # topic survives if gap <= 2 days

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def load_keywords(conn: PGConnection) -> pd.DataFrame:
    sql = """
        SELECT date, word, SUM(count) AS freq
        FROM keywords_daily
        WHERE source <> 'ALL'
        GROUP BY date, word
        ORDER BY date;
    """
    return pd.read_sql(sql, conn)

KeywordLifetimeRow = tuple[str, Any, Any, int, int]

def compute_lifetime(df: pd.DataFrame) -> list[KeywordLifetimeRow]:
    df["date"] = pd.to_datetime(df["date"])

    lifetimes = []

    for word, group in df.groupby("word"):
        g = group.sort_values("date")

        # fill gaps (0 freq)
        all_days = pd.date_range(g["date"].min(), g["date"].max(), freq="D")
        merged = pd.DataFrame({"date": all_days})
        merged = merged.merge(g, on="date", how="left").fillna({"freq": 0})

        # detect episodes
        start = None
        last_date = None
        total = 0

        for r in merged.itertuples():
            if start is None:
                if r.freq > 0:
                    start = r.date
                    total = r.freq
                last_date = r.date
                continue

            gap = (r.date - last_date).days

            if gap <= GAP_THRESHOLD:
                total += r.freq
            else:
                # close episode
                lifetimes.append((
                    word, start.date(), last_date.date(),
                    (last_date - start).days + 1, int(total)
                ))
                start = r.date if r.freq > 0 else None
                total = r.freq

            last_date = r.date

        if start is not None:
            lifetimes.append((
                word, start.date(), last_date.date(),
                (last_date - start).days + 1, int(total)
            ))

    return lifetimes

def save(conn: PGConnection, rows: list[KeywordLifetimeRow]) -> None:
    sql = """
        CREATE TABLE IF NOT EXISTS keyword_lifetime (
            word TEXT,
            start_date DATE,
            end_date DATE,
            duration_days INTEGER,
            total_frequency INTEGER
        );
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        execute_values(
            cur,
            "INSERT INTO keyword_lifetime VALUES %s",
            rows
        )
    conn.commit()

def main() -> None:
    conn = get_conn()
    try:
        df = load_keywords(conn)
        if df.empty:
            logger.info("No keyword data.")
            return

        rows = compute_lifetime(df)
        save(conn, rows)
        logger.info("Keyword lifetime COMPLETE.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
