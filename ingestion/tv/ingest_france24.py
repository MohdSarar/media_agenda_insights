from core.db import get_conn
import os
import datetime as dt
from core.logging import get_logger


import feedparser

import yaml
from dotenv import load_dotenv

from typing import Any, Mapping, Optional, TypedDict
import datetime as dt
from core.db_types import PGConnection
from core.schemas import RSSArticle


class ParsedEntry(TypedDict):
    title: str
    summary: str
    url: str
    published_at: dt.datetime

# Chargement de l'environnement
load_dotenv()

logger = get_logger(__name__)


DB_URL = os.getenv("DATABASE_URL")

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),  # racine du projet
    "infra", "config", "feeds_france24.yaml"
)


def load_feeds_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


get_db_connection = get_conn

def parse_entry(entry: Mapping[str, Any]) -> Optional[ParsedEntry]:
    """
    Normalise les champs importants d'une entrée RSS/Atom.
    """
    title = entry.get("title", "").strip()

    summary = entry.get("summary") or entry.get("description") or ""
    summary = summary.strip()

    url = entry.get("link") or entry.get("id")
    if not url:
        return None

    published = None
    if "published_parsed" in entry and entry.published_parsed:
        published = dt.datetime(*entry.published_parsed[:6])
    elif "updated_parsed" in entry and entry.updated_parsed:
        published = dt.datetime(*entry.updated_parsed[:6])
    else:
        published = dt.datetime.utcnow()

    return {
        "title": title,
        "summary": summary,
        "url": url,
        "published_at": published,
    }


def ingest_france24_feeds() -> None:
    feeds_cfg = load_feeds_config(CONFIG_PATH)
    logger.info(f"[F24] Chargement des flux France 24 depuis {CONFIG_PATH}")

    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()

        inserted_count = 0

        try:
            for source_key, source_info in feeds_cfg.items():
                label = source_info.get("label", source_key)
                lang = source_info.get("lang", "fr")
                media_type = source_info.get("media_type", "tv")
                feeds = source_info.get("feeds", [])

                for feed in feeds:
                    feed_name = feed.get("name")
                    feed_url = feed.get("url")

                    if not feed_url:
                        logger.warning(f"[F24][{label}] Feed '{feed_name}' sans URL, ignoré.")
                        continue

                    logger.info(f"[F24] Ingestion {label}/{feed_name} : {feed_url}")
                    parsed = feedparser.parse(feed_url)

                    if parsed.bozo:
                        logger.warning(
                            f"[F24] Problème de parsing pour {feed_url}: {parsed.bozo_exception}"
                        )

                    for entry in parsed.entries:
                        data = parse_entry(entry)
                        if not data:
                            continue

                        # ✅ Validation Pydantic AVANT DB
                        raw_data = {
                            "source": source_key,          
                            "category": feed_name,         
                            "title": data["title"],
                            "content": data["summary"],
                            "url": data["url"],
                            "published_at": data["published_at"],
                            "lang": lang,                   
                        }

                        try:
                            article = RSSArticle(**raw_data)
                        except Exception as e:
                            logger.warning("[F24] Invalid article skipped (url=%s): %s", raw_data.get("url"), e)
                            continue

                        try:
                            cur.execute(
                                """
                                INSERT INTO articles_raw_f24
                                    (source, lang, media_type, feed_name,
                                    title, summary, url, published_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (url) DO NOTHING
                                """,
                                (
                                    source_key,
                                    article.lang,
                                    media_type,
                                    feed_name,
                                    article.title,
                                    article.content,
                                    str(article.url),
                                    article.published_at,
                                )
                            )
                            if cur.rowcount > 0:
                                inserted_count += 1
                        except Exception as e:
                            logger.error(
                                f"[F24] Erreur insertion (source={source_key}, url={data['url']}): {e}"
                            )

            conn.commit()
            logger.info(f"[F24] Ingestion terminée. Nouveaux articles insérés : {inserted_count}")

        except Exception as e:
            conn.rollback()
            logger.error(f"[F24] Erreur pendant l'ingestion France 24, rollback: {e}")
            raise
        finally:
            cur.close()
            


if __name__ == "__main__":
    ingest_france24_feeds()
