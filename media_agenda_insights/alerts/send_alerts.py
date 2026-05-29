"""
Watchlist alert sender.

Reads watchlist_terms from the DB, computes Z-score spikes for the last
N days, and sends Telegram notifications for new spikes — without
re-alerting if already sent today (idempotent via alerts_sent table).

Usage:
    python alerts/send_alerts.py [--days 7] [--dry-run]

Env vars required for Telegram:
    TELEGRAM_BOT_TOKEN  — bot API token (from @BotFather)
    TELEGRAM_CHAT_ID    — chat/channel ID to send to

If the Telegram vars are not set, alerts are only logged to stdout.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import urllib.request
import urllib.parse
import json
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db import get_conn  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Z_THRESHOLD = 2.0  # standard deviations above baseline to trigger alert


def _ensure_tables(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_terms (
            id       SERIAL PRIMARY KEY,
            term     TEXT NOT NULL UNIQUE,
            added_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS alerts_sent (
            id         BIGSERIAL PRIMARY KEY,
            term       TEXT    NOT NULL,
            alert_date DATE    NOT NULL,
            z_score    FLOAT,
            channel    TEXT    NOT NULL DEFAULT 'telegram',
            sent_at    TIMESTAMP DEFAULT NOW(),
            UNIQUE (term, alert_date, channel)
        );
    """)


def _fetch_watchlist(cur) -> list[str]:
    cur.execute("SELECT term FROM watchlist_terms ORDER BY term")
    return [r[0] for r in cur.fetchall()]


def _fetch_trend(cur, term: str, days_back: int) -> list[tuple[date, int]]:
    """Return [(date, total_mentions)] sorted ascending."""
    since = date.today() - timedelta(days=days_back + 30)
    cur.execute("""
        SELECT date, SUM(count) AS n
        FROM keywords_daily
        WHERE word = %s AND date >= %s AND media_type = 'tv'
        GROUP BY date ORDER BY date ASC
    """, (term, since))
    return cur.fetchall()


def _detect_spike(rows: list[tuple[date, int]], window: int) -> dict | None:
    if len(rows) < window + 3:
        return None
    values = [r[1] for r in rows]
    baseline = values[:-window]
    recent = values[-window:]
    mu = sum(baseline) / len(baseline)
    variance = sum((x - mu) ** 2 for x in baseline) / len(baseline)
    sigma = variance ** 0.5 or 1.0
    recent_mean = sum(recent) / len(recent)
    z = (recent_mean - mu) / sigma
    if z >= Z_THRESHOLD:
        peak_row = max(rows[-window:], key=lambda r: r[1])
        return {
            "z_score": round(z, 2),
            "recent_avg": round(recent_mean, 1),
            "baseline_avg": round(mu, 1),
            "peak_date": peak_row[0],
        }
    return None


def _already_sent(cur, term: str, channel: str) -> bool:
    cur.execute("""
        SELECT 1 FROM alerts_sent
        WHERE term = %s AND alert_date = %s AND channel = %s
    """, (term, date.today(), channel))
    return cur.fetchone() is not None


def _record_sent(cur, term: str, z_score: float, channel: str) -> None:
    cur.execute("""
        INSERT INTO alerts_sent (term, alert_date, z_score, channel)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (term, alert_date, channel) DO NOTHING
    """, (term, date.today(), z_score, channel))


def _send_telegram(token: str, chat_id: str, text: str) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as exc:
        logger.warning("Telegram request failed: %s", exc)
        return False


def run(window_days: int = 7, dry_run: bool = False) -> None:
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID")
    channel = "telegram" if (tg_token and tg_chat) else "log"

    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        _ensure_tables(cur)

        terms = _fetch_watchlist(cur)
        logger.info("Watchlist: %d terms — window %d days", len(terms), window_days)

        sent = 0
        for term in terms:
            rows = _fetch_trend(cur, term, days_back=window_days)
            spike = _detect_spike(rows, window=window_days)
            if not spike:
                continue

            logger.info(
                "SPIKE detected: %s  z=%.2f  recent_avg=%.1f  baseline=%.1f",
                term, spike["z_score"], spike["recent_avg"], spike["baseline_avg"],
            )

            if dry_run:
                continue

            if _already_sent(cur, term, channel):
                logger.info("Already alerted today for '%s' on channel '%s'.", term, channel)
                continue

            msg = (
                f"⚡ *Alerte Watchlist* — `{term}`\n"
                f"Z-score : *{spike['z_score']}* σ\n"
                f"Moyenne récente ({window_days}j) : {spike['recent_avg']:.0f} mentions/j\n"
                f"Baseline : {spike['baseline_avg']:.0f} mentions/j\n"
                f"Pic le : {spike['peak_date']}"
            )

            ok = True
            if tg_token and tg_chat:
                ok = _send_telegram(tg_token, tg_chat, msg)
            else:
                logger.info("[LOG ONLY] %s", msg.replace("\n", " | "))

            if ok:
                _record_sent(cur, term, spike["z_score"], channel)
                sent += 1

        conn.commit()
        cur.close()

    logger.info("Done. %d alert(s) sent.", sent)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="Spike detection window (days)")
    parser.add_argument("--dry-run", action="store_true", help="Detect but do not send or record")
    args = parser.parse_args()
    run(window_days=args.days, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
