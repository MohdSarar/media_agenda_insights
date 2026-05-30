
from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from typing import Iterator


_pool: SimpleConnectionPool | None = None


def _init_pool() -> SimpleConnectionPool:
    dsn = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Missing DATABASE_URL (or DB_URL) in .env")
    return SimpleConnectionPool(1, 5, dsn)



@contextmanager
def get_conn() -> Iterator[psycopg2.extensions.connection]:
    global _pool
    if _pool is None:
        _pool = _init_pool()

    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)
