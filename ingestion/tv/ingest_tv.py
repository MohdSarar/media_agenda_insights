from core.db import get_conn
import os
import datetime as dt
from core.http import fetch_url_text
from core.logging import get_logger


import feedparser

import yaml
from dotenv import load_dotenv
from typing import Any, Mapping, Optional, TypedDict
import datetime as dt
from core.db_types import PGConnection, PGCursor
from core.schemas import RSSArticle


# Chargement des variables d'environnement (.env)
load_dotenv()

logger = get_logger(__name__)


class ParsedEntry(TypedDict):
    title: str
    summary: str
    url: str
    published_at: dt.datetime


DB_URL = os.getenv("DATABASE_URL")

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),  # remonte à la racine
    "infra", "config", "feeds_tv.yaml"
)


def load_feeds_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


get_db_connection = get_conn

def parse_entry(entry: Mapping[str, Any]) -> Optional[ParsedEntry]:
    """
    Normalise les champs importants d'une entrée RSS/Atom.
    On gère plusieurs formats possibles sans se baser sur un site spécifique.
    """
    title = entry.get("title", "").strip()

    # Certains flux utilisent 'summary', d'autres 'description'
    summary = entry.get("summary") or entry.get("description") or ""
    summary = summary.strip()

    url = entry.get("link")
    if not url:
        # Certains flux peuvent utiliser 'id' ou autre, on garde une fallback
        url = entry.get("id")
    if not url:
        return None  # on ignore si on n'a vraiment pas de lien

    # Date de publication
    published = None
    if "published_parsed" in entry and entry.published_parsed:
        published = dt.datetime(*entry.published_parsed[:6])
    elif "updated_parsed" in entry and entry.updated_parsed:
        published = dt.datetime(*entry.updated_parsed[:6])
    else:
        # fallback : maintenant (mieux que rien)
        published = dt.datetime.utcnow()

    return {
        "title": title,
        "summary": summary,
        "url": url,
        "published_at": published
    }


from core.db import get_conn  # make sure this import exists

def ingest_tv_feeds() -> None:
    feeds_cfg = load_feeds_config(CONFIG_PATH)
    logger.info(f"Chargement des flux TV depuis {CONFIG_PATH}")

    inserted_count = 0

    with get_conn() as conn:
        conn.autocommit = False

        try:
            with conn.cursor() as cur:
                for source_key, source_info in feeds_cfg.items():
                    label = source_info.get("label", source_key)
                    feeds = source_info.get("feeds", [])

                    for feed in feeds:
                        feed_name = feed.get("name")
                        feed_url = feed.get("url")

                        if not feed_url:
                            logger.warning(f"[{label}] Feed '{feed_name}' sans URL, ignoré.")
                            continue

                        logger.info(f"Ingestion feed {label}/{feed_name} : {feed_url}")

                        xml = fetch_url_text(feed_url)
                        parsed = feedparser.parse(xml)

                        if parsed.bozo:
                            logger.warning(
                                f"Problème de parsing pour {feed_url}: {parsed.bozo_exception}"
                            )

                        for entry in parsed.entries:
                            data = parse_entry(entry)
                            if not data:
                                continue

                            raw_data = {
                                "source": source_key,
                                "category": feed_name,
                                "title": data["title"],
                                "content": data["summary"],
                                "url": data["url"],
                                "published_at": data["published_at"],
                                "lang": "fr",
                            }

                            try:
                                article = RSSArticle(**raw_data)
                            except Exception as e:
                                logger.warning(
                                    "Invalid TV article skipped (url=%s): %s",
                                    raw_data.get("url"),
                                    e,
                                )
                                continue

                            try:
                                cur.execute(
                                    """
                                    INSERT INTO articles_raw
                                    (source, media_type, feed_name, title, summary, url, published_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (url) DO NOTHING
                                    """,
                                    (
                                        source_key,
                                        "tv",
                                        feed_name,
                                        article.title,
                                        article.content,
                                        str(article.url),
                                        article.published_at,
                                    ),
                                )
                                if cur.rowcount > 0:
                                    inserted_count += 1
                            except Exception as e:
                                logger.error(
                                    f"Erreur insertion article (source={source_key}, url={data['url']}): {e}"
                                )

            conn.commit()
            logger.info(f"Ingestion terminée. Nouveaux articles insérés : {inserted_count}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur pendant l'ingestion TV, rollback effectué: {e}")
            raise


if __name__ == "__main__":
    ingest_tv_feeds()
