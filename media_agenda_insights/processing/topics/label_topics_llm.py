# processing/topics/label_topics_llm.py
#
# Generates human-readable topic labels via Claude Haiku.
# Writes results to the llm_label column (added if missing).
# Original topic_label (keyword list) is preserved untouched.

from __future__ import annotations

import asyncio
import os

from anthropic import AsyncAnthropic
from psycopg2.extras import execute_batch

from core.db import get_conn
from core.logging import get_logger

logger = get_logger(__name__)

MODEL = "claude-haiku-4-5-20251001"

_PROMPTS = {
    "fr": "Mots-clés TV/presse française : {kw}\nTitre de sujet court (5 mots max, français) :",
    "en": "News keywords: {kw}\nShort topic headline (5 words max, English):",
    "es": "Palabras clave periodísticas: {kw}\nTítulo corto (5 palabras máx, español):",
    "ar":  "كلمات مفتاحية إعلامية: {kw}\nعنوان موضوع قصير (5 كلمات كحد أقصى، عربي):",
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
                    logger.info(f"Rate limit hit — waiting {wait:.0f}s before retry {attempt + 1}/4")
                    await asyncio.sleep(wait)
                elif attempt == 3:
                    logger.warning(f"LLM failed permanently for {keywords[:3]}: {e}")
                    return key, None
                else:
                    await asyncio.sleep(2 ** attempt)
        return key, None


async def _run_async(
    pairs: list[tuple[tuple, list[str], str]],
    concurrency: int,
) -> dict[tuple, str]:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in environment")
    client = AsyncAnthropic(api_key=api_key)
    sem = asyncio.Semaphore(concurrency)
    # 50 RPM limit → each worker must wait 1.2s × concurrency after completing,
    # so total throughput = concurrency / (api_time + delay) ≤ 50/60 req/s
    delay = 1.2 * concurrency
    total = len(pairs)
    done_count = 0
    lock = asyncio.Lock()

    async def _tracked(key, kw, lang):
        nonlocal done_count
        result = await _call_llm(client, sem, key, kw, lang, delay)
        async with lock:
            done_count += 1
            if done_count % 50 == 0 or done_count == total:
                logger.info(f"  progress: {done_count}/{total} labels generated")
        return result

    tasks = [_tracked(key, kw, lang) for key, kw, lang in pairs]
    results = await asyncio.gather(*tasks)
    # Only keep successful labels (None = failed, will be retried on next run)
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


def label_table(
    table: str,
    lang_col: str | None,
    dry_run: bool = False,
    concurrency: int = 5,
) -> int:
    """
    Labels all unlabeled rows in `table`.
    Returns the number of unique API calls made (or that would be made in dry-run).

    Uses three short-lived DB connections so the connection is never held open
    during the LLM phase (Neon closes idle connections after ~5 minutes).
    """
    # Phase 1 — read (short connection)
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        _ensure_llm_label_column(cur, table)
        conn.commit()
        rows = _fetch_unlabeled(cur, table, lang_col)
        cur.close()
    # connection released here

    if not rows:
        logger.info(f"{table}: all rows already labeled.")
        return 0

    # Deduplicate on (sorted keywords, lang) — same content = one API call
    unique: dict[tuple, tuple[list[str], str]] = {}
    for row in rows:
        key = (tuple(sorted(row["kw"])), row["lang"])
        if key not in unique:
            unique[key] = (row["kw"], row["lang"])

    logger.info(f"{table}: {len(rows)} rows → {len(unique)} unique keyword sets.")

    if dry_run:
        for key, (kw, lang) in list(unique.items())[:3]:
            logger.info(f"  sample [{lang}]: {kw[:5]}")
        return len(unique)

    # Phase 2 — LLM calls (no DB connection held)
    pairs = [(k, kw, lang) for k, (kw, lang) in unique.items()]
    label_map = asyncio.run(_run_async(pairs, concurrency=concurrency))

    # Map back to row ids — skip rows whose label failed (keep llm_label NULL for retry)
    updates = []
    skipped = 0
    for row in rows:
        key = (tuple(sorted(row["kw"])), row["lang"])
        label = label_map.get(key)
        if label is None:
            skipped += 1
            continue
        updates.append((label, row["id"]))
    if skipped:
        logger.info(f"{table}: {skipped} rows skipped (API failure) — will retry on next run.")

    # Phase 3 — write results (fresh connection)
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
        logger.info(f"{table}: {len(updates)} llm_label values written.")
        cur.close()

    return len(unique)
