#!/usr/bin/env python3
"""
backfill_topic_labels.py
========================
Generates human-readable topic labels via Claude Haiku for all historical topics.

Adds llm_label column to topics_daily and topics_daily_f24 (safe, non-destructive).
Original topic_label (keyword list) is preserved and used as fallback.

Usage
-----
  python backfill_topic_labels.py --dry-run    # preview counts + cost estimate
  python backfill_topic_labels.py --confirm    # run for real
"""

from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv
load_dotenv()

from core.logging import get_logger
from processing.topics.label_topics_llm import label_table

logger = get_logger(__name__)

# Claude Haiku pricing (input+output combined estimate)
_TOKENS_PER_CALL = 60
_PRICE_PER_1M = 0.25


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill LLM topic labels.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show counts and cost estimate, make no changes.")
    parser.add_argument("--confirm", action="store_true",
                        help="Required to actually generate and write labels.")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  LLM TOPIC LABELING — Claude Haiku")
    print("=" * 60)

    if args.dry_run:
        print("\n[DRY RUN] Counting rows and unique keyword sets...\n")
        n_tv  = label_table("topics_daily",      lang_col=None,   dry_run=True)
        n_f24 = label_table("topics_daily_f24",  lang_col="lang", dry_run=True)
        total = n_tv + n_f24
        cost  = total * _TOKENS_PER_1M * _PRICE_PER_1M / 1_000_000 if False else total * _TOKENS_PER_CALL * _PRICE_PER_1M / 1_000_000
        print(f"\n  topics_daily     : {n_tv:,} unique API calls needed")
        print(f"  topics_daily_f24 : {n_f24:,} unique API calls needed")
        print(f"  Total API calls  : {total:,}")
        print(f"  Estimated cost   : ~${cost:.2f}  (Claude Haiku)")
        print(f"\n  To run for real  : python backfill_topic_labels.py --confirm")
        print("=" * 60 + "\n")
        return

    if not args.confirm:
        print("\n  Add --confirm to generate labels, or --dry-run to preview.")
        print("  Nothing was changed.")
        sys.exit(0)

    print("\nStep 1/2 — TV/press topics (topics_daily)...")
    n_tv = label_table("topics_daily", lang_col=None)
    print(f"  ✓ {n_tv:,} unique labels generated.")

    print("\nStep 2/2 — France 24 topics (topics_daily_f24)...")
    n_f24 = label_table("topics_daily_f24", lang_col="lang")
    print(f"  ✓ {n_f24:,} unique labels generated.")

    total = n_tv + n_f24
    cost  = total * _TOKENS_PER_CALL * _PRICE_PER_1M / 1_000_000
    print(f"\n{'=' * 60}")
    print(f"  Done. {total:,} labels written (~${cost:.2f} spent).")
    print(f"  Dashboard will now show llm_label when available,")
    print(f"  falling back to keyword list for any unlabeled rows.")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
