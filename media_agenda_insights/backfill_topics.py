#!/usr/bin/env python3
"""
backfill_topics.py
==================
Wipes the two topic tables and re-extracts from scratch using
the updated label width (7 keywords instead of 3).

ONLY these two tables are touched:
  - topics_daily        (TV / press)
  - topics_daily_f24    (France 24)

These tables are NEVER touched (source of truth):
  - articles_raw          articles_raw_f24
  - articles_clean        articles_clean_f24
  - keywords_daily        keywords_daily_f24
  - (all other tables)

Usage
-----
  # Dry-run: shows counts, does nothing
  python backfill_topics.py --dry-run

  # Real run: asks for --confirm before proceeding
  python backfill_topics.py --confirm
"""

from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv
load_dotenv()

from core.db import get_conn
from core.logging import get_logger

logger = get_logger(__name__)

_TARGET_TABLES = ("topics_daily", "topics_daily_f24")

_PROTECTED_TABLES = (
    "articles_raw", "articles_clean",
    "articles_raw_f24", "articles_clean_f24",
    "keywords_daily", "keywords_daily_f24",
)


def _count_rows(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table};")
    return cur.fetchone()[0]


def _source_data_summary(cur) -> None:
    print("\n  Source tables (will NOT be modified):")
    for t in ("articles_raw", "articles_clean", "articles_raw_f24", "articles_clean_f24"):
        try:
            n = _count_rows(cur, t)
            print(f"    {t}: {n:,} rows  ✓")
        except Exception:
            print(f"    {t}: (inaccessible)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill topic tables.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print current state and exit without making any changes.")
    parser.add_argument("--confirm", action="store_true",
                        help="Required flag to actually run the truncation and re-extraction.")
    args = parser.parse_args()

    # ── Step 1: show current state ────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  TOPIC BACKFILL — current state")
    print("=" * 60)

    with get_conn() as conn:
        cur = conn.cursor()

        print("\n  Topic tables (will be rebuilt):")
        before: dict[str, int] = {}
        for t in _TARGET_TABLES:
            n = _count_rows(cur, t)
            before[t] = n
            print(f"    {t}: {n:,} rows")

        _source_data_summary(cur)
        cur.close()

    print("\n" + "-" * 60)
    print("  This will TRUNCATE topics_daily and topics_daily_f24")
    print("  then re-extract all topics from articles_clean.")
    print("  No other table will be touched.")
    print("-" * 60)

    if args.dry_run:
        print("\n[DRY RUN] Nothing was changed.")
        print("  To run for real: python backfill_topics.py --confirm")
        print("=" * 60 + "\n")
        return

    if not args.confirm:
        print("\n  Add --confirm to actually run the backfill.")
        print("  Example: python backfill_topics.py --confirm")
        print("\nNothing was changed.")
        sys.exit(0)

    # ── Step 2: truncate ──────────────────────────────────────────────────────
    print("\nTruncating topic tables...")
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        try:
            for t in _TARGET_TABLES:
                cur.execute(f"TRUNCATE {t};")
                print(f"  ✓ TRUNCATE {t}")
            conn.commit()
            print("  Commit OK.")
        except Exception as e:
            conn.rollback()
            print(f"\n  ERROR during truncation: {e}")
            print("  Rolled back. Nothing was deleted.")
            cur.close()
            sys.exit(1)
        finally:
            cur.close()

    # ── Step 3: re-extract TV / press topics ──────────────────────────────────
    print("\nRe-extracting TV/press topics (extract_topics.py)...")
    try:
        from processing.topics.extract_topics import compute_topics_daily
        compute_topics_daily()
        print("  ✓ TV/press topics done.")
    except Exception as e:
        print(f"\n  ERROR in extract_topics: {e}")
        print("  topics_daily may be partially filled.")
        print("  Re-run 'python backfill_topics.py --confirm' to retry.")
        sys.exit(1)

    # ── Step 4: re-extract France 24 topics ───────────────────────────────────
    print("\nRe-extracting France 24 topics (extract_france24_topics.py)...")
    try:
        from processing.topics.extract_france24_topics import compute_france24_topics_daily
        compute_france24_topics_daily()
        print("  ✓ France 24 topics done.")
    except Exception as e:
        print(f"\n  ERROR in extract_france24_topics: {e}")
        print("  topics_daily_f24 may be partially filled.")
        print("  Re-run 'python backfill_topics.py --confirm' to retry.")
        sys.exit(1)

    # ── Step 5: final counts ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Done — final state")
    print("=" * 60)
    with get_conn() as conn:
        cur = conn.cursor()
        for t in _TARGET_TABLES:
            after = _count_rows(cur, t)
            gain = after - before.get(t, 0)
            sign = "+" if gain >= 0 else ""
            print(f"  {t}: {after:,} rows  (was {before[t]:,}, {sign}{gain:,})")
        _source_data_summary(cur)
        cur.close()
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
