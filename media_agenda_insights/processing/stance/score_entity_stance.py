"""
Score named-entity stance per (entity, source, date).

For each article with NER annotations we compute document-level
sentiment from lemma co-occurrence with a curated French lexicon,
then attribute that sentiment to every entity mentioned in the article.
Results are upserted into `entity_stance_daily`.

Usage:
    python score_entity_stance.py --date 2025-05-01
    python score_entity_stance.py --start 2025-04-01 --end 2025-04-30
    python score_entity_stance.py --date 2025-05-01 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Sentiment lexicon (French, news/politics domain) ─────────────────────────

POSITIVE = frozenset([
    "victoire", "succès", "accord", "paix", "progrès", "croissance", "soutien",
    "réforme", "espoir", "aide", "dialogue", "réconciliation", "développement",
    "amélioration", "efficace", "résolution", "investissement", "liberté",
    "protection", "innovation", "solidarité", "engagement", "renforcement",
    "défense", "stabilité", "réussi", "excellent", "favorable", "positif",
    "avancée", "bénéfice", "coopération", "gain", "hausse", "rebond",
    "relance", "rétablissement", "sauvé", "sécurisé", "triomphe", "unanime",
    "unité", "valorisé", "volontaire", "démocratique", "transparent",
])

NEGATIVE = frozenset([
    "échec", "crise", "guerre", "conflit", "attentat", "meurtre", "mort",
    "violence", "scandale", "fraude", "corruption", "arrestation", "condamné",
    "accusé", "danger", "menace", "problème", "catastrophe", "défaite",
    "explosion", "victime", "terrorisme", "polémique", "démission",
    "licenciement", "faillite", "procès", "détention", "incendie", "blessé",
    "blessés", "morts", "tué", "tués", "attaque", "bombardement", "émeute",
    "grève", "blocage", "contestation", "rejet", "abandon", "perte", "chute",
    "effondrement", "accusation", "mise-en-examen", "complot", "impasse",
])


def _score_lemmas(lemmas: list) -> tuple[int, int]:
    """Return (positive_count, negative_count) for a lemma list."""
    pos = sum(1 for l in lemmas if l and l.lower() in POSITIVE)
    neg = sum(1 for l in lemmas if l and l.lower() in NEGATIVE)
    return pos, neg


def _ensure_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS entity_stance_daily (
            id BIGSERIAL PRIMARY KEY,
            entity_text  TEXT NOT NULL,
            entity_label TEXT NOT NULL,
            source       TEXT NOT NULL,
            date         DATE NOT NULL,
            positive_count INT NOT NULL DEFAULT 0,
            negative_count INT NOT NULL DEFAULT 0,
            mention_count  INT NOT NULL DEFAULT 0,
            UNIQUE (entity_text, entity_label, source, date)
        );
        CREATE INDEX IF NOT EXISTS idx_esd_date_source
            ON entity_stance_daily (date, source);
        CREATE INDEX IF NOT EXISTS idx_esd_entity
            ON entity_stance_daily (entity_text);
    """)


def score_range(start: date, end: date, dry_run: bool = False) -> int:
    """Score entities for every day in [start, end]. Returns total rows upserted."""
    from core.db import get_conn
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        _ensure_table(cur)

        # Fetch articles with entities in the date range
        cur.execute("""
            SELECT
                ar.source,
                ar.published_at::date AS art_date,
                ac.lemmas,
                ac.entities
            FROM articles_clean ac
            JOIN articles_raw ar ON ar.id = ac.article_id
            WHERE ar.published_at::date BETWEEN %s AND %s
              AND ac.entities IS NOT NULL
              AND jsonb_array_length(ac.entities) > 0
            ORDER BY ar.published_at::date ASC
        """, (start, end))
        rows = cur.fetchall()

        logger.info("Fetched %d articles with entities (%s → %s)", len(rows), start, end)

        # Aggregate per (entity_text, entity_label, source, date)
        # Key → (positive_sum, negative_sum, mention_count)
        agg: dict[tuple, list[int]] = defaultdict(lambda: [0, 0, 0])

        for source, art_date, lemmas, entities_raw in rows:
            if not lemmas and not entities_raw:
                continue

            lemma_list: list[str] = lemmas if isinstance(lemmas, list) else []
            pos, neg = _score_lemmas(lemma_list)

            # entities_raw may arrive as str (JSON) or already parsed
            if isinstance(entities_raw, str):
                try:
                    entities = json.loads(entities_raw)
                except Exception:
                    continue
            else:
                entities = entities_raw or []

            for ent in entities:
                if not isinstance(ent, dict):
                    continue
                text = (ent.get("text") or "").strip()
                label = (ent.get("label") or "").strip()
                if not text or len(text) < 3:
                    continue
                key = (text, label, source, art_date)
                agg[key][0] += pos
                agg[key][1] += neg
                agg[key][2] += 1

        if not agg:
            logger.info("No entity stance data to write.")
            conn.rollback()
            return 0

        records = [
            (text, label, source, d, pos, neg, count)
            for (text, label, source, d), (pos, neg, count) in agg.items()
        ]

        logger.info("Upserting %d (entity, source, date) records…", len(records))

        if not dry_run:
            execute_values(
                cur,
                """
                INSERT INTO entity_stance_daily
                    (entity_text, entity_label, source, date,
                     positive_count, negative_count, mention_count)
                VALUES %s
                ON CONFLICT (entity_text, entity_label, source, date) DO UPDATE SET
                    positive_count = EXCLUDED.positive_count,
                    negative_count = EXCLUDED.negative_count,
                    mention_count  = EXCLUDED.mention_count
                """,
                records,
                page_size=500,
            )
            conn.commit()
            logger.info("Done. %d rows upserted.", len(records))
        else:
            logger.info("[DRY-RUN] Would upsert %d rows.", len(records))
            conn.rollback()

        cur.close()
        return len(records)


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="Single date YYYY-MM-DD")
    group.add_argument("--start", help="Start date YYYY-MM-DD (use with --end)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.date:
        d = date.fromisoformat(args.date)
        score_range(d, d, dry_run=args.dry_run)
    else:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end) if args.end else date.today()
        score_range(start, end, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
