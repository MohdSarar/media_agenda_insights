#!/usr/bin/env python3
"""
purge_old_data.py
=================
Three-in-one maintenance script — run once initially, then periodically.

  A. Idempotent DDL migrations (safe to re-run):
       DROP tokens  from articles_clean, articles_clean_f24, social_posts_clean
       DROP lemmas  from social_posts_clean
     (columns were write-only; no downstream code reads them)

  B. One-time data patch:
       SET raw_json = NULL in social_posts_raw
     (column kept for schema compatibility; no downstream code reads it)

  C. Rolling retention purge  (raw_days from infra/config/pipeline.yaml, default 90):
       DELETE articles_raw       older than cutoff  → cascades to articles_clean + narratives_assignments
       DELETE articles_raw_f24   older than cutoff  → cascades to articles_clean_f24
       DELETE social_posts_raw   older than cutoff  (no cascade)
       DELETE social_posts_clean older than cutoff  (purge on processed_at, independent)

Analytics tables (keywords_daily, topics_daily, etc.) are NEVER touched.

Usage:
  python purge_old_data.py --dry-run    # preview counts, no writes
  python purge_old_data.py --confirm    # apply all changes
"""

from __future__ import annotations

import argparse
import os
import sys

import yaml
from dotenv import load_dotenv

from core.db import get_conn
from core.logging import get_logger

load_dotenv()
logger = get_logger(__name__)

_CONFIG_PATH = os.getenv("PIPELINE_CONFIG", "infra/config/pipeline.yaml")


def _load_raw_days() -> int:
    try:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        return int(cfg.get("retention", {}).get("raw_days", 90))
    except Exception:
        return 90


def _column_exists(cur, table: str, col: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        """,
        (table, col),
    )
    return cur.fetchone()[0] > 0


# ---------------------------------------------------------------------------
# --dry-run
# ---------------------------------------------------------------------------

def _dry_run(raw_days: int) -> None:
    interval = f"{raw_days} days"
    with get_conn() as conn:
        cur = conn.cursor()

        print(f"\n{'='*62}")
        print(f"  DRY RUN — retention window: {raw_days} days")
        print(f"{'='*62}\n")

        # A. DDL
        print("A) DDL migrations (idempotent — skipped if already applied):")
        for table, col in [
            ("articles_clean",     "tokens"),
            ("articles_clean_f24", "tokens"),
            ("social_posts_clean", "tokens"),
            ("social_posts_clean", "lemmas"),
        ]:
            status = "will DROP" if _column_exists(cur, table, col) else "already gone ✓"
            print(f"   {table}.{col:<8}  {status}")

        # B. raw_json
        cur.execute("SELECT COUNT(*) FROM social_posts_raw WHERE raw_json IS NOT NULL")
        n_raw_json = cur.fetchone()[0]
        print(f"\nB) raw_json cleanup:")
        print(f"   social_posts_raw rows with raw_json IS NOT NULL: {n_raw_json:,}")

        # C. Retention
        print(f"\nC) Retention purge (older than {raw_days} days):")

        cur.execute("SELECT COUNT(*) FROM articles_raw WHERE published_at < NOW() - INTERVAL %s", (interval,))
        n_raw = cur.fetchone()[0]
        cur.execute(
            """
            SELECT COUNT(*) FROM articles_clean ac
            JOIN articles_raw ar ON ar.id = ac.article_id
            WHERE ar.published_at < NOW() - INTERVAL %s
            """,
            (interval,),
        )
        n_clean = cur.fetchone()[0]
        cur.execute(
            """
            SELECT COUNT(*) FROM narratives_assignments na
            JOIN articles_raw ar ON ar.id = na.article_id
            WHERE ar.published_at < NOW() - INTERVAL %s
            """,
            (interval,),
        )
        n_narr = cur.fetchone()[0]
        print(
            f"   articles_raw:          {n_raw:>7,}  "
            f"→ cascade: {n_clean:,} articles_clean, {n_narr:,} narratives_assignments"
        )

        cur.execute(
            "SELECT COUNT(*) FROM articles_raw_f24 WHERE published_at < NOW() - INTERVAL %s", (interval,)
        )
        n_raw_f24 = cur.fetchone()[0]
        cur.execute(
            """
            SELECT COUNT(*) FROM articles_clean_f24 ac
            JOIN articles_raw_f24 ar ON ar.id = ac.article_id
            WHERE ar.published_at < NOW() - INTERVAL %s
            """,
            (interval,),
        )
        n_clean_f24 = cur.fetchone()[0]
        print(f"   articles_raw_f24:      {n_raw_f24:>7,}  → cascade: {n_clean_f24:,} articles_clean_f24")

        cur.execute(
            "SELECT COUNT(*) FROM social_posts_raw WHERE published_at < NOW() - INTERVAL %s", (interval,)
        )
        n_social_raw = cur.fetchone()[0]
        print(f"   social_posts_raw:      {n_social_raw:>7,}  (no cascade)")

        cur.execute(
            "SELECT COUNT(*) FROM social_posts_clean WHERE processed_at < NOW() - INTERVAL %s", (interval,)
        )
        n_social_clean = cur.fetchone()[0]
        print(f"   social_posts_clean:    {n_social_clean:>7,}  (purge on processed_at)")

        print(f"\n  Run with --confirm to apply.")
        print(f"{'='*62}\n")
        cur.close()


# ---------------------------------------------------------------------------
# --confirm
# ---------------------------------------------------------------------------

def _confirm(raw_days: int) -> None:
    interval = f"{raw_days} days"
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()

        print(f"\n{'='*62}")
        print(f"  CONFIRM — applying all changes (retention: {raw_days} days)")
        print(f"{'='*62}\n")

        # A. DDL migrations
        print("A) Applying DDL migrations...")
        for table, col in [
            ("articles_clean",     "tokens"),
            ("articles_clean_f24", "tokens"),
            ("social_posts_clean", "tokens"),
            ("social_posts_clean", "lemmas"),
        ]:
            cur.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {col};")
            logger.info(f"DDL: DROP COLUMN IF EXISTS {table}.{col}")
        conn.commit()
        print("   Committed.\n")

        # B. raw_json cleanup
        print("B) Clearing raw_json...")
        cur.execute("UPDATE social_posts_raw SET raw_json = NULL WHERE raw_json IS NOT NULL")
        n = cur.rowcount
        conn.commit()
        logger.info(f"raw_json cleared from {n:,} rows in social_posts_raw.")
        print(f"   {n:,} rows patched.\n")

        # C. Retention purge
        print(f"C) Purging rows older than {raw_days} days...")

        cur.execute(
            "DELETE FROM articles_raw WHERE published_at < NOW() - INTERVAL %s", (interval,)
        )
        n_raw = cur.rowcount
        conn.commit()
        logger.info(f"Deleted {n_raw:,} from articles_raw (cascade: articles_clean + narratives_assignments).")
        print(f"   articles_raw:          {n_raw:,} deleted (+ cascades)")

        cur.execute(
            "DELETE FROM articles_raw_f24 WHERE published_at < NOW() - INTERVAL %s", (interval,)
        )
        n_raw_f24 = cur.rowcount
        conn.commit()
        logger.info(f"Deleted {n_raw_f24:,} from articles_raw_f24 (cascade: articles_clean_f24).")
        print(f"   articles_raw_f24:      {n_raw_f24:,} deleted (+ cascade)")

        cur.execute(
            "DELETE FROM social_posts_raw WHERE published_at < NOW() - INTERVAL %s", (interval,)
        )
        n_social_raw = cur.rowcount
        conn.commit()
        logger.info(f"Deleted {n_social_raw:,} from social_posts_raw.")
        print(f"   social_posts_raw:      {n_social_raw:,} deleted")

        cur.execute(
            "DELETE FROM social_posts_clean WHERE processed_at < NOW() - INTERVAL %s", (interval,)
        )
        n_social_clean = cur.rowcount
        conn.commit()
        logger.info(f"Deleted {n_social_clean:,} from social_posts_clean.")
        print(f"   social_posts_clean:    {n_social_clean:,} deleted")

        cur.close()
        print(f"\n{'='*62}")
        print(f"  Done.")
        print(f"{'='*62}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="DB maintenance: DDL migrations + raw_json cleanup + retention purge."
    )
    parser.add_argument("--dry-run", action="store_true", help="Show counts, no writes.")
    parser.add_argument("--confirm", action="store_true", help="Apply all changes.")
    args = parser.parse_args()

    raw_days = _load_raw_days()

    if args.dry_run:
        _dry_run(raw_days)
    elif args.confirm:
        _confirm(raw_days)
    else:
        print("\n  Add --dry-run to preview, or --confirm to apply.")
        print("  Nothing was changed.\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
