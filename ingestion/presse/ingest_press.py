# ingestion/presse/ingest_press.py

import os
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
import yaml
import feedparser

# Charger les variables d'environnement (DATABASE_URL)
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Résolution du chemin du fichier YAML (indépendant de Windows/Linux)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "infra" / "config" / "feeds_press.yaml"


def get_db_connection():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL manquant dans l'environnement")
    return psycopg2.connect(DB_URL)


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


def parse_published(entry) -> Optional[datetime]:
    """
    Convertit la date RSS en datetime naive compatible PostgreSQL.
    Retourne None si pas de date exploitable.
    """
    if getattr(entry, "published_parsed", None):
        ts = time.mktime(entry.published_parsed)
        return datetime.fromtimestamp(ts)
    if getattr(entry, "updated_parsed", None):
        ts = time.mktime(entry.updated_parsed)
        return datetime.fromtimestamp(ts)
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


def ingest_press():
    """
    Ingestion des flux RSS de presse définis dans feeds_press.yaml
    vers la table articles_raw (media_type = 'press').
    """
    press_cfg = load_press_feeds()
    conn = get_db_connection()
    conn.autocommit = False

    total_inserted = 0

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            for source_key, source_cfg in press_cfg.items():
                label = source_cfg.get("label", source_key)
                feeds = source_cfg.get("feeds", [])

                if not isinstance(feeds, list):
                    logging.warning("Section 'feeds' invalide pour %s, ignorée.", source_key)
                    continue

                for feed in feeds:
                    feed_name = feed.get("name", "default")
                    feed_url = feed.get("url")

                    if not feed_url:
                        logging.warning(
                            "Flux sans URL pour %s / %s, ignoré.",
                            source_key, feed_name
                        )
                        continue

                    logging.info("Lecture flux presse %s (%s) - %s", label, feed_name, feed_url)

                    parsed = feedparser.parse(feed_url)

                    if parsed.bozo:
                        logging.warning(
                            "Flux mal formé ou erreur réseau pour %s : %s",
                            feed_url,
                            getattr(parsed, "bozo_exception", None),
                        )

                    for entry in parsed.entries:
                        title = (getattr(entry, "title", "") or "").strip()
                        summary = (getattr(entry, "summary", "") or "").strip()
                        article_url = getattr(entry, "link", None)

                        if not title:
                            # On ignore les entrées sans titre
                            continue

                        published_at = parse_published(entry)

                        # Dédoublonnage par URL si disponible
                        if article_url:
                            cur.execute(
                                "SELECT 1 FROM articles_raw WHERE url = %s LIMIT 1;",
                                (article_url,),
                            )
                            if cur.fetchone():
                                # Déjà vu, peu importe le flux (une, politique, etc.)
                                continue

                        # Sinon, fallback sur les règles titre + date
                        elif published_at is not None:
                            if article_exists_with_date(cur, source_key, feed_name, title, published_at):
                                continue
                        else:
                            if article_exists_without_date(cur, source_key, feed_name, title):
                                continue

                        # Insertion
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
                                source_key,          # ex: lemonde, lefigaro...
                                "press",             # media_type
                                feed_name,           # ex: une, politique
                                title,
                                summary,
                                article_url,         # stocké dans la colonne 'url' de la table
                                None,                # raw_content (scraping full article plus tard)
                                published_at,
                            ),
                        )
                        total_inserted += 1

        conn.commit()
        logging.info("Ingestion presse terminée. Nouveaux articles insérés : %d", total_inserted)

    except Exception as e:
        conn.rollback()
        logging.error("Erreur durant l'ingestion presse : %s", e)
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    ingest_press()
