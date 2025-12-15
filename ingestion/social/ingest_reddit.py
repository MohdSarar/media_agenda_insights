from core.db import get_conn
import os
import json
import time
from core.logging import get_logger

import datetime as dt

import requests
import yaml
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from core.http import fetch_json



from typing import Any, Mapping, Optional, TypedDict
import datetime as dt
from core.db_types import PGConnection, JsonDict

class NormalizedRedditPost(TypedDict):
    post_id: str
    title: str
    content: str
    author: Optional[str]
    url: str
    published_at: Optional[dt.datetime]
    raw: JsonDict

# Chargement des variables d'environnement (.env)
load_dotenv()

logger = get_logger(__name__)


DB_URL = os.getenv("DATABASE_URL")
SOCIAL_CFG_PATH = os.getenv("SOCIAL_CFG_PATH", "infra/config/feeds_social.yaml")

REDDIT_BASE = "https://www.reddit.com"
DEFAULT_TIMEOUT = 20
DEFAULT_SLEEP_S = 1.2  # petit throttle anti-rate-limit

# User-Agent OBLIGATOIRE pour Reddit (sinon 429/blocked fréquemment)
USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "MediaAgendaInsights/1.0 (contact: you@example.com)"
)


def load_config(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config introuvable: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


connect_db = get_conn

def reddit_fetch(subreddit: str, mode: str = "new", limit: int = 100) -> JsonDict:
    mode = (mode or "new").strip().lower()
    if mode not in {"new", "hot", "top", "rising"}:
        mode = "new"

    url = f"{REDDIT_BASE}/r/{subreddit}/{mode}.json"
    params = {"limit": int(limit)}
    headers = {"User-Agent": USER_AGENT}

    data = fetch_json(url, params=params, headers=headers)
    return data


def normalize_post(child: Mapping[str, Any]) -> Optional[NormalizedRedditPost]:
    """
    Transforme un post Reddit en ligne social_posts_raw.
    On reste sur signaux publics (title/selftext/url/author/date).
    """
    data = (child or {}).get("data") or {}
    post_id = data.get("id")
    if not post_id:
        return None

    title = (data.get("title") or "").strip()
    selftext = (data.get("selftext") or "").strip()
    author = data.get("author")
    permalink = data.get("permalink") or ""
    url = data.get("url") or (REDDIT_BASE + permalink if permalink else None)

    created_utc = data.get("created_utc")
    published_at = None
    if created_utc:
        try:
            published_at = dt.datetime.utcfromtimestamp(float(created_utc))
        except Exception:
            published_at = None

    # on conserve le payload brut, utile pour debug/évolutions
    raw_json = data

    return {
        "external_id": str(post_id),
        "url": url,
        "title": title if title else None,
        "content": selftext if selftext else None,
        "author": author,
        "published_at": published_at,
        "raw_json": raw_json,
    }


def insert_posts(
    conn: PGConnection,
    platform: str,
    source: str,
    posts: list[NormalizedRedditPost],
) -> tuple[int, int]:
    """
    Insère en DB avec idempotence.
    Retourne (inserted_count, skipped_count).
    """
    inserted = 0
    skipped = 0

    sql = """
        INSERT INTO social_posts_raw
            (platform, source, external_id, url, title, content, author, published_at, lang_guess, raw_json)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (platform, external_id) DO NOTHING
    """

    with conn.cursor() as cur:
        for p in posts:
            # lang_guess laissé vide ici : on le fait proprement au NLP (process_social_posts.py)
            cur.execute(
                sql,
                (
                    platform,
                    source,
                    p["external_id"],
                    p["url"],
                    p["title"],
                    p["content"],
                    p["author"],
                    p["published_at"],
                    None,
                    Json(p["raw_json"]),
                )
            )
            # psycopg2: rowcount = 1 si insertion, 0 si conflict
            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

    conn.commit()
    return inserted, skipped


def main() -> None:
    cfg = load_config(SOCIAL_CFG_PATH)
    reddit_cfg = (cfg or {}).get("reddit") or {}

    if not reddit_cfg.get("enabled", True):
        logger.info("Reddit ingestion désactivée (reddit.enabled=false).")
        return

    sources = reddit_cfg.get("sources") or []
    if not sources:
        logger.warning("Aucune source Reddit dans feeds_social.yaml (reddit.sources).")
        return

    with get_conn() as conn:
        
        total_inserted = 0
        total_skipped = 0

   
        for src in sources:
            name = (src.get("name") or "").strip()
            subreddit = (src.get("subreddit") or "").strip()
            mode = (src.get("mode") or "new").strip()
            limit = int(src.get("limit") or 100)

            if not subreddit:
                logger.warning("Source Reddit ignorée (subreddit manquant) : %s", src)
                continue

            source_key = name if name else subreddit
            logger.info("Fetching Reddit: r/%s (%s, limit=%s)", subreddit, mode, limit)

            data = reddit_fetch(subreddit=subreddit, mode=mode, limit=limit)
            children = (((data or {}).get("data") or {}).get("children")) or []

            posts = []
            for child in children:
                p = normalize_post(child)
                if p:
                    posts.append(p)

            ins, skp = insert_posts(conn, platform="reddit", source=source_key, posts=posts)
            total_inserted += ins
            total_skipped += skp

            logger.info("Reddit r/%s -> inserted=%s skipped=%s (fetched=%s)", subreddit, ins, skp, len(posts))
            time.sleep(DEFAULT_SLEEP_S)

        logger.info("DONE Reddit ingestion. total_inserted=%s total_skipped=%s", total_inserted, total_skipped)

    


if __name__ == "__main__":
    main()
