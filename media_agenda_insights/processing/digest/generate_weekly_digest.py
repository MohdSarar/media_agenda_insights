"""
Generate a weekly media intelligence digest via LLM.

Aggregates top topics, keywords, entities, and divergence for the given
week, then asks Claude Haiku to write a 5-bullet French editorial summary.
The result is stored in `weekly_digests` (one row per week_start date).

Usage:
    # Current week (Mon–today)
    python generate_weekly_digest.py

    # Specific week
    python generate_weekly_digest.py --week 2025-05-19

    # Dry-run (print prompt + response, no DB write)
    python generate_weekly_digest.py --week 2025-05-19 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 600


def _week_bounds(week_start: date) -> tuple[date, date]:
    monday = week_start - timedelta(days=week_start.weekday())
    return monday, monday + timedelta(days=6)


def _ensure_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weekly_digests (
            id          BIGSERIAL PRIMARY KEY,
            week_start  DATE NOT NULL UNIQUE,
            week_end    DATE NOT NULL,
            digest_text TEXT NOT NULL,
            context_json JSONB,
            generated_at TIMESTAMP DEFAULT NOW()
        );
    """)


def _fetch_context(cur, start: date, end: date) -> dict:
    """Pull aggregated stats for the week to feed into the LLM prompt."""

    # Top 5 TV topics
    cur.execute("""
        SELECT COALESCE(llm_label, topic_label) AS label, SUM(articles_count) AS n
        FROM topics_daily
        WHERE date BETWEEN %s AND %s
          AND media_type = 'tv' AND source = 'ALL'
          AND topic_label IS NOT NULL
        GROUP BY label ORDER BY n DESC LIMIT 5
    """, (start, end))
    top_topics = [{"label": r[0], "articles": r[1]} for r in cur.fetchall()]

    # Top 10 TV keywords
    cur.execute("""
        SELECT word, SUM(count) AS n
        FROM keywords_daily
        WHERE date BETWEEN %s AND %s AND media_type = 'tv' AND source != 'ALL'
        GROUP BY word ORDER BY n DESC LIMIT 10
    """, (start, end))
    top_keywords = [r[0] for r in cur.fetchall()]

    # Top 5 mentioned persons
    cur.execute("""
        SELECT (ent->>'text') AS entity, COUNT(*) AS n
        FROM articles_raw ar
        JOIN articles_clean ac ON ac.article_id = ar.id
        CROSS JOIN LATERAL jsonb_array_elements(ac.entities) AS ent
        WHERE ar.published_at::date BETWEEN %s AND %s
          AND ar.media_type = 'tv'
          AND ent->>'label' = 'PER'
          AND LENGTH(COALESCE(ent->>'text', '')) > 2
        GROUP BY entity ORDER BY n DESC LIMIT 5
    """, (start, end))
    top_persons = [r[0] for r in cur.fetchall()]

    # Article counts per source
    cur.execute("""
        SELECT source, COUNT(*) AS n
        FROM articles_raw
        WHERE published_at::date BETWEEN %s AND %s AND media_type = 'tv'
        GROUP BY source ORDER BY n DESC
    """, (start, end))
    source_counts = {r[0]: r[1] for r in cur.fetchall()}

    return {
        "week": f"{start} → {end}",
        "top_topics": top_topics,
        "top_keywords": top_keywords,
        "top_persons": top_persons,
        "source_counts": source_counts,
    }


def _build_prompt(ctx: dict) -> str:
    topics_str = "\n".join(
        f"  {i+1}. {t['label']} ({t['articles']} articles)"
        for i, t in enumerate(ctx["top_topics"])
    ) or "  (aucun)"
    kw_str = ", ".join(ctx["top_keywords"]) or "(aucun)"
    persons_str = ", ".join(ctx["top_persons"]) or "(aucun)"
    sources_str = ", ".join(f"{s}: {n}" for s, n in list(ctx["source_counts"].items())[:6])

    return f"""Tu es un analyste médias senior spécialisé dans l'intelligence de l'agenda médiatique français.

Voici les données de couverture télévisuelle pour la semaine du {ctx['week']} :

SUJETS DOMINANTS (TV) :
{topics_str}

MOTS-CLÉS LES PLUS FRÉQUENTS :
{kw_str}

PERSONNALITÉS LES PLUS MENTIONNÉES :
{persons_str}

VOLUME PAR CHAÎNE (articles) :
{sources_str}

Rédige un **digest éditorial en 5 points** (bullets «•») en français, en 150 mots maximum.
Chaque point doit mettre en lumière un angle journalistique ou un biais notable.
Ne répète pas les chiffres bruts — analyse et interprète.
Commence directement par le premier «•»."""


def _call_llm(prompt: str) -> str:
    import anthropic as _anthropic
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set.")
    client = _anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()


def generate(week_start: date, dry_run: bool = False) -> str:
    from core.db import get_conn
    start, end = _week_bounds(week_start)
    logger.info("Generating digest for week %s → %s", start, end)

    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        _ensure_table(cur)

        ctx = _fetch_context(cur, start, end)
        logger.info(
            "Context: %d topics, %d keywords, %d persons, %d sources",
            len(ctx["top_topics"]),
            len(ctx["top_keywords"]),
            len(ctx["top_persons"]),
            len(ctx["source_counts"]),
        )

        if not ctx["top_topics"] and not ctx["top_keywords"]:
            logger.warning("No data for this week — skipping digest generation.")
            conn.rollback()
            cur.close()
            return ""

        prompt = _build_prompt(ctx)
        logger.info("Calling LLM (%s)…", MODEL)
        digest_text = _call_llm(prompt)

        if dry_run:
            logger.info("[DRY-RUN] Digest:\n%s", digest_text)
            conn.rollback()
            cur.close()
            return digest_text

        cur.execute("""
            INSERT INTO weekly_digests (week_start, week_end, digest_text, context_json)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (week_start) DO UPDATE SET
                digest_text  = EXCLUDED.digest_text,
                context_json = EXCLUDED.context_json,
                generated_at = NOW()
        """, (start, end, digest_text, json.dumps(ctx)))
        conn.commit()
        logger.info("Digest saved (week_start=%s).", start)
        cur.close()

    return digest_text


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--week",
        help="Any date in the target week (YYYY-MM-DD). Default: current week.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    ref = date.fromisoformat(args.week) if args.week else date.today()
    generate(ref, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
