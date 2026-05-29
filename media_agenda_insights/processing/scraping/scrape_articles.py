# processing/scraping/scrape_articles.py
#
# Fetches full article text for articles published in the last LOOKBACK_DAYS.
# Stores result in articles_raw.full_text (added if missing).
# Cleans up full_text for articles older than LOOKBACK_DAYS to keep storage stable.
#
# Run:  python -m processing.scraping.scrape_articles

from __future__ import annotations

import os
import time
from collections import defaultdict
from urllib.parse import urlparse

import trafilatura
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

from core.db import get_conn
from core.http import fetch_url_text
from core.logging import get_logger

load_dotenv()
logger = get_logger(__name__)

LOOKBACK_DAYS = int(os.getenv("SCRAPE_LOOKBACK_DAYS", "30"))
DOMAIN_DELAY  = float(os.getenv("SCRAPE_DELAY_PER_DOMAIN", "2.0"))   # seconds between requests to same domain
BATCH_SIZE    = 50                                                      # commit every N articles
MIN_TEXT_LEN  = 150                                                     # discard extracted text shorter than this


def _ensure_full_text_column(cur) -> None:
    cur.execute(
        "ALTER TABLE articles_raw ADD COLUMN IF NOT EXISTS full_text TEXT DEFAULT NULL;"
    )


def _fetch_to_scrape(cur) -> list[dict]:
    cur.execute(
        """
        SELECT id, url, source
        FROM articles_raw
        WHERE published_at >= NOW() - INTERVAL %s
          AND full_text IS NULL
          AND url IS NOT NULL
          AND url <> ''
        ORDER BY published_at DESC
        """,
        (f"{LOOKBACK_DAYS} days",),
    )
    return [{"id": r[0], "url": r[1], "source": r[2]} for r in cur.fetchall()]


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return "unknown"


def _extract(url: str) -> str | None:
    try:
        html = fetch_url_text(url)
        if not html:
            return None
        text = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            favor_precision=True,
            deduplicate=True,
        )
        if text and len(text) >= MIN_TEXT_LEN:
            return text
        return None
    except Exception as e:
        logger.debug(f"Extraction failed [{url}]: {e}")
        return None


def cleanup_old_full_text() -> None:
    """Set full_text = NULL for articles older than LOOKBACK_DAYS to keep storage stable."""
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        _ensure_full_text_column(cur)
        conn.commit()
        cur.execute(
            """
            UPDATE articles_raw
            SET full_text = NULL
            WHERE published_at < NOW() - INTERVAL %s
              AND full_text IS NOT NULL
            """,
            (f"{LOOKBACK_DAYS} days",),
        )
        n = cur.rowcount
        conn.commit()
        cur.close()
        if n:
            logger.info(f"Cleared full_text from {n} articles older than {LOOKBACK_DAYS} days.")


def scrape_recent_articles() -> None:
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()

        _ensure_full_text_column(cur)
        conn.commit()

        articles = _fetch_to_scrape(cur)
        if not articles:
            logger.info(f"No articles to scrape in the last {LOOKBACK_DAYS} days.")
            cur.close()
            return

        logger.info(f"{len(articles)} articles to scrape (last {LOOKBACK_DAYS} days).")

        domain_last: dict[str, float] = defaultdict(float)
        updates: list[tuple] = []
        ok = fail = 0

        for i, art in enumerate(articles, 1):
            url = art["url"]
            dom = _domain(url)

            # Per-domain rate limiting
            wait = DOMAIN_DELAY - (time.time() - domain_last[dom])
            if wait > 0:
                time.sleep(wait)
            domain_last[dom] = time.time()

            text = _extract(url)
            if text:
                updates.append((text, art["id"]))
                ok += 1
            else:
                fail += 1

            # Commit in batches
            if len(updates) >= BATCH_SIZE:
                execute_batch(
                    cur,
                    "UPDATE articles_raw SET full_text = %s WHERE id = %s",
                    updates,
                    page_size=BATCH_SIZE,
                )
                conn.commit()
                updates = []
                logger.info(f"[{i}/{len(articles)}] {ok} scraped, {fail} failed")

        # Final batch
        if updates:
            execute_batch(
                cur,
                "UPDATE articles_raw SET full_text = %s WHERE id = %s",
                updates,
                page_size=BATCH_SIZE,
            )
            conn.commit()

        cur.close()
        logger.info(f"Scraping complete: {ok} scraped, {fail} skipped (paywall/dead/too short).")


if __name__ == "__main__":
    cleanup_old_full_text()
    scrape_recent_articles()
