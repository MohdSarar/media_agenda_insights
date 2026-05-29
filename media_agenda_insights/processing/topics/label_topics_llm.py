# processing/topics/label_topics_llm.py
#
# Generates human-readable topic labels via Claude Haiku.
# Writes results to the llm_label column (added if missing).
# Original topic_label (keyword list) is preserved untouched.
#
# Processes in batches of BATCH_SIZE unique keyword sets, writing to DB
# after each batch — safe to Ctrl+C and resume at any time.

from __future__ import annotations

import asyncio
import os

from anthropic import AsyncAnthropic
from psycopg2.extras import execute_batch

from core.db import get_conn
from core.logging import get_logger

logger = get_logger(__name__)

MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 100  # unique keyword sets per batch; DB commit after each

_PROMPTS = {
    "fr": "Mots-clés TV/presse française : {kw}\nTitre de sujet court (5 mots max, français) :",
    "en": "News keywords: {kw}\nShort topic headline (5 words max, English):",
    "es": "Palabras clave periodísticas: {kw}\nTítulo corto (5 palabras máx, español):",
    "ar": "كلمات مفتاحية إعلامية: {kw}\nعنوان موضوع قصير (5 كلمات كحد أقصى، عربي):",
}


def _build_prompt(keywords: list[str], lang: str) -> str:
    tmpl = _PROMPTS.get(lang, _PROMPTS["fr"])
    return tmpl.format(kw=", ".join(keywords[:7]))


async def _call_llm(
    client: AsyncAnthropic,
    sem: asyncio.Semaphore,
    key: tuple,
    keywords: list[str],
    lang: str,
    delay: float,
) -> tuple[tuple, str | None]:
    """Returns (key, label) or (key, None) on unrecoverable failure."""
    async with sem:
        for attempt in range(4):
            try:
                msg = await client.messages.create(
                    model=MODEL,
                    max_tokens=25,
                    messages=[{"role": "user", "content": _build_prompt(keywords, lang)}],
                )
                label = msg.content[0].text.strip().rstrip(".,;:")
                await asyncio.sleep(delay)
                return key, label
            except Exception as e:
                err_str = str(e)
                if "rate_limit" in err_str:
                    wait = 60 / max(1, attempt + 1)
                    logger.info(f"Rate limit — waiting {wait:.0f}s (attempt {attempt + 1}/4)")
                    await asyncio.sleep(wait)
                elif attempt == 3:
                    logger.warning(f"LLM failed permanently for {keywords[:3]}: {e}")
                    return key, None
                else:
                    await asyncio.sleep(2 ** attempt)
        return key, None


async def _run_batch(
    pairs: list[tuple[tuple, list[str], str]],
    concurrency: int,
) -> dict[tuple, str]:
    """Run LLM calls for one batch. Returns key→label for successes."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in environment")
    client = AsyncAnthropic(api_key=api_key)
    sem = asyncio.Semaphore(concurrency)
    # 50 RPM: each worker waits 1.2s × concurrency so total rate ≤ 50/60 req/s
    delay = 1.2 * concurrency
    results = await asyncio.gather(
        *[_call_llm(client, sem, key, kw, lang, delay) for key, kw, lang in pairs]
    )
    return {k: v for k, v in results if v is not None}


def _ensure_llm_label_column(cur, table: str) -> None:
    cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS llm_label TEXT DEFAULT NULL;")


def _fetch_unlabeled(cur, table: str, lang_col: str | None) -> list[dict]:
    if lang_col:
        cur.execute(f"""
            SELECT id, keywords, {lang_col}
            FROM {table}
            WHERE llm_label IS NULL
              AND keywords IS NOT NULL
              AND array_length(keywords, 1) > 0
        """)
        return [{"id": r[0], "kw": list(r[1]), "lang": (r[2] or "fr")[:2]} for r in cur.fetchall()]
    else:
        cur.execute(f"""
            SELECT id, keywords
            FROM {table}
            WHERE llm_label IS NULL
              AND keywords IS NOT NULL
              AND array_length(keywords, 1) > 0
        """)
        return [{"id": r[0], "kw": list(r[1]), "lang": "fr"} for r in cur.fetchall()]


def _write_batch(table: str, updates: list[tuple]) -> None:
    """Open a fresh connection and write a batch of (label, id) pairs."""
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        execute_batch(
            cur,
            f"UPDATE {table} SET llm_label = %s WHERE id = %s",
            updates,
            page_size=500,
        )
        conn.commit()
        cur.close()


def label_table(
    table: str,
    lang_col: str | None,
    dry_run: bool = False,
    concurrency: int = 1,
) -> int:
    """
    Labels all unlabeled rows in `table` in batches of BATCH_SIZE.
    Commits to DB after every batch — safe to interrupt and resume.
    Returns the number of unique keyword sets processed.
    """
    # Phase 1 — fetch unlabeled rows (short connection, released immediately)
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        _ensure_llm_label_column(cur, table)
        conn.commit()
        rows = _fetch_unlabeled(cur, table, lang_col)
        cur.close()

    if not rows:
        logger.info(f"{table}: all rows already labeled.")
        return 0

    # Deduplicate: same keyword set → one API call
    unique: dict[tuple, tuple[list[str], str]] = {}
    for row in rows:
        key = (tuple(sorted(row["kw"])), row["lang"])
        if key not in unique:
            unique[key] = (row["kw"], row["lang"])

    # Build key → [row_ids] index for fast writes
    key_to_ids: dict[tuple, list[int]] = {}
    for row in rows:
        key = (tuple(sorted(row["kw"])), row["lang"])
        key_to_ids.setdefault(key, []).append(row["id"])

    n_unique = len(unique)
    logger.info(f"{table}: {len(rows)} rows → {n_unique} unique keyword sets.")

    if dry_run:
        for key, (kw, lang) in list(unique.items())[:3]:
            logger.info(f"  sample [{lang}]: {kw[:5]}")
        return n_unique

    # Phase 2 — batch loop: LLM → DB commit → next batch
    all_pairs = [(k, kw, lang) for k, (kw, lang) in unique.items()]
    n_batches = (n_unique + BATCH_SIZE - 1) // BATCH_SIZE
    total_written = 0
    total_skipped = 0

    for batch_num, start in enumerate(range(0, n_unique, BATCH_SIZE), 1):
        batch_pairs = all_pairs[start: start + BATCH_SIZE]
        logger.info(
            f"{table}: batch {batch_num}/{n_batches} "
            f"({len(batch_pairs)} sets, {total_written} written so far)"
        )

        # LLM calls — no DB connection held
        label_map = asyncio.run(_run_batch(batch_pairs, concurrency=concurrency))

        # Build updates for this batch
        updates: list[tuple] = []
        for key, _, _ in batch_pairs:
            label = label_map.get(key)
            if label is None:
                total_skipped += len(key_to_ids.get(key, []))
                continue
            for row_id in key_to_ids.get(key, []):
                updates.append((label, row_id))

        # Commit immediately
        if updates:
            _write_batch(table, updates)
            total_written += len(updates)

        logger.info(
            f"{table}: batch {batch_num}/{n_batches} done — "
            f"{total_written} labels written, {total_skipped} skipped"
        )

    logger.info(
        f"{table}: complete — {total_written} written, {total_skipped} skipped (will retry on next run)."
    )
    return n_unique
