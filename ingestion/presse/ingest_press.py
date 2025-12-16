from core.db import get_conn
# ingestion/presse/ingest_press.py

import os
from core.http import fetch_url_text
from core.logging import get_logger
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
import yaml
import feedparser

from typing import Any, Mapping, Optional
from datetime import datetime
from core.db_types import PGConnection
from datetime import datetime, timezone
from core.schemas import RSSArticle
import calendar


# Charger les variables d'environnement (DATABASE_URL)
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

logger = get_logger(__name__)


# Résolution du chemin du fichier YAML (indépendant de Windows/Linux)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "infra" / "config" / "feeds_press.yaml"


get_db_connection = get_conn

def load_press_feeds() -> Dict[str, Any]:
    """
    Charge la config des flux de presse depuis infra/config/feeds_press.yaml.

    Structure attendue :
    press:
      lemonde:
        label: "Le Monde"
        feeds:
          - name: "une"
            url: "https://..."
    """
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Fichier de config introuvable : {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict) or "press" not in cfg:
        raise RuntimeError("Configuration feeds_press.yaml invalide : clé 'press' manquante")

    press_cfg = cfg["press"]
    if not isinstance(press_cfg, dict):
        raise RuntimeError("La section 'press' de feeds_press.yaml doit être un dictionnaire")

    return press_cfg


def parse_published(entry: Mapping[str, Any]) -> Optional[datetime]:
    """
    Convertit la date RSS en datetime naive (UTC) compatible PostgreSQL.
    Retourne None si pas de date exploitable.
    """
    parsed = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not parsed:
        return None

    try:
        # feedparser donne un struct_time généralement en UTC
        ts = calendar.timegm(parsed)
        return datetime.utcfromtimestamp(ts)  # naive UTC
    except (OverflowError, OSError, ValueError):
        return None


def article_exists_with_date(
    cur,
    source: str,
    feed_name: str,
    title: str,
    published_at: datetime,
) -> bool:
    """
    Vérifie si un article existe déjà avec source + flux + titre + date.
    """
    cur.execute(
        """
        SELECT 1
        FROM articles_raw
        WHERE source = %s
          AND feed_name = %s
          AND title = %s
          AND published_at = %s
        LIMIT 1;
        """,
        (source, feed_name, title, published_at),
    )
    return cur.fetchone() is not None


def article_exists_without_date(
    cur,
    source: str,
    feed_name: str,
    title: str,
) -> bool:
    """
    Vérifie si un article existe déjà sans tenir compte de la date
    (utile si le flux n'en fournit pas).
    """
    cur.execute(
        """
        SELECT 1
        FROM articles_raw
        WHERE source = %s
          AND feed_name = %s
          AND title = %s
        LIMIT 1;
        """,
        (source, feed_name, title),
    )
    return cur.fetchone() is not None


def ingest_press() -> None:
    """
    Ingestion des flux RSS de presse définis dans feeds_press.yaml
    vers la table articles_raw (media_type = 'press').
    """
    press_cfg = load_press_feeds()

    total_inserted = 0

    with get_conn() as conn:
        conn.autocommit = False

        try:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                for source_key, source_cfg in press_cfg.items():
                    label = source_cfg.get("label", source_key)
                    feeds = source_cfg.get("feeds", [])

                    if not isinstance(feeds, list):
                        logger.warning("Section 'feeds' invalide pour %s, ignorée.", source_key)
                        continue
                    failed_feeds = 0
            
                    for feed in feeds:
                        feed_name = feed.get("name", "default")
                        feed_url = feed.get("url")

                        if not feed_url:
                            logger.warning(
                                "Flux sans URL pour %s / %s, ignoré.",
                                source_key, feed_name
                            )
                            continue

                        logger.info("Lecture flux presse %s (%s) - %s", label, feed_name, feed_url)

                        try:
                            xml = fetch_url_text(feed_url)
                            if not xml:
                                logger.warning("Feed vide, ignoré: %s", feed_url)
                                continue

                        except Exception as e:
                            logger.error(
                                "Feed presse skipped (fetch failed): %s | %s",
                                feed_url,
                                str(e),
                            )
                            failed_feeds += 1

                            continue

                        parsed = feedparser.parse(xml)

                        if parsed.bozo:
                            logger.warning(
                                "Flux mal formé ou erreur réseau pour %s : %s",
                                feed_url,
                                getattr(parsed, "bozo_exception", None),
                            )

                        for entry in parsed.entries:
                            title = (getattr(entry, "title", "") or "").strip()
                            summary = (getattr(entry, "summary", "") or "").strip()
                            article_url = getattr(entry, "link", None)

                            if not title:
                                continue

                            published_at = parse_published(entry)

                            if published_at is None:
                                published_at = datetime.now(timezone.utc)

                            if not article_url:
                                logger.warning(
                                    "Entrée presse ignorée (url manquante) source=%s feed=%s title=%s",
                                    source_key, feed_name, title[:120]
                                )
                                continue

                            raw_data = {
                                "source": source_key,
                                "category": feed_name,
                                "title": title,
                                "content": summary,
                                "url": article_url,
                                "published_at": published_at,
                                "lang": "fr",
                            }

                            try:
                                article = RSSArticle(**raw_data)
                            except Exception as e:
                                logger.warning("Invalid press article skipped (url=%s): %s", article_url, e)
                                continue

                            cur.execute(
                                "SELECT 1 FROM articles_raw WHERE url = %s LIMIT 1;",
                                (str(article.url),),
                            )
                            if cur.fetchone():
                                continue

                            cur.execute(
                                """
                                INSERT INTO articles_raw (
                                    source,
                                    media_type,
                                    feed_name,
                                    title,
                                    summary,
                                    url,
                                    raw_content,
                                    published_at
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                                """,
                                (
                                    source_key,
                                    "press",
                                    feed_name,
                                    article.title,
                                    article.content,
                                    str(article.url),
                                    None,
                                    article.published_at,
                                ),
                            )
                            total_inserted += 1

            conn.commit()
            logger.info(
                "Ingestion presse terminée. Nouveaux articles insérés : %d",
                total_inserted,
                extra={"failed_feeds": failed_feeds},
            )


        except Exception as e:
            conn.rollback()
            logger.error("Erreur durant l'ingestion presse : %s", e)
            raise


if __name__ == "__main__":
    ingest_press()
