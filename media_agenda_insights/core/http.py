from __future__ import annotations
import logging

from dataclasses import dataclass
from typing import Iterable, Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from core.config import CONFIG
from core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class HttpRetryConfig:
    timeout_seconds: int
    max_attempts: int
    backoff_min_seconds: int
    backoff_max_seconds: int
    retry_status_codes: tuple[int, ...]
    user_agent: str


def _load_http_config() -> HttpRetryConfig:
    http_cfg = CONFIG.get("http", {}) or {}
    return HttpRetryConfig(
        timeout_seconds=int(http_cfg.get("timeout_seconds", 15)),
        max_attempts=int(http_cfg.get("max_attempts", 5)),
        backoff_min_seconds=int(http_cfg.get("backoff_min_seconds", 1)),
        backoff_max_seconds=int(http_cfg.get("backoff_max_seconds", 20)),
        retry_status_codes=tuple(http_cfg.get("retry_status_codes", [429, 500, 502, 503, 504])),
        user_agent=str(http_cfg.get("user_agent", "MediaAgendaInsights/1.0")),
    )


def _should_retry_response(resp: Optional[requests.Response]) -> bool:
    if resp is None:
        return True
    return resp.status_code in _load_http_config().retry_status_codes

from tenacity import RetryCallState
import logging

def log_retry(retry_state: RetryCallState) -> None:
    """
    Hook Tenacity compatible avec logging JSON custom.
    """
    logger.warning(
        "HTTP retry",
        extra={
            "attempt": retry_state.attempt_number,
            "wait_seconds": retry_state.next_action.sleep if retry_state.next_action else None,
            "exception": str(retry_state.outcome.exception())
            if retry_state.outcome and retry_state.outcome.failed
            else None,
        },
    )


def fetch_url_text(url: str, *, session: Optional[requests.Session] = None) -> str:
    cfg = _load_http_config()
    sess = session or requests.Session()
    headers = {"User-Agent": cfg.user_agent}

    @retry(
        reraise=True,
        stop=stop_after_attempt(cfg.max_attempts),
        wait=wait_exponential(min=cfg.backoff_min_seconds, max=cfg.backoff_max_seconds),
        retry=(
            retry_if_exception_type((requests.exceptions.Timeout, requests.exceptions.ConnectionError))
            | retry_if_result(_should_retry_response)
        ),
        before_sleep=log_retry,
    )
    def _do_get() -> requests.Response:
        resp = sess.get(url, headers=headers, timeout=cfg.timeout_seconds)
        # 404/410: feed absent => on skip (pas d'exception)
        if resp.status_code in (404, 410):
            return resp
        # 4xx hors 429: non récupérable => on retourne pour skip
        if 400 <= resp.status_code < 500 and resp.status_code != 429:
            return resp
        # 5xx/429: _should_retry_response va déclencher retry
        return resp

    resp = _do_get()

    # Skip propre sur 4xx non-retryables
    if resp.status_code in (404, 410) or (400 <= resp.status_code < 500 and resp.status_code != 429):
        logger.warning("HTTP client error (skip)", extra={"url": url, "status_code": resp.status_code})
        return ""

    resp.raise_for_status()
    return resp.text



from typing import Any, Optional, Mapping

def fetch_json(
    url: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, str]] = None,
    session: Optional[requests.Session] = None,
) -> Any:
    """
    Fetch HTTP GET -> JSON with retries + exponential backoff.
    Supports params/headers (needed for Reddit).
    """
    cfg = _load_http_config()
    sess = session or requests.Session()

    final_headers = {"User-Agent": cfg.user_agent}
    if headers:
        final_headers.update(dict(headers))

    @retry(
        reraise=True,
        stop=stop_after_attempt(cfg.max_attempts),
        wait=wait_exponential(min=cfg.backoff_min_seconds, max=cfg.backoff_max_seconds),
        retry=(
            retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.ConnectionError, requests.exceptions.Timeout))
            | retry_if_result(_should_retry_response)
        ),
        before_sleep=log_retry,
    )
    def _do_get() -> requests.Response:
        resp = sess.get(url, params=params, headers=final_headers, timeout=cfg.timeout_seconds)
        if resp.status_code >= 400 and resp.status_code not in cfg.retry_status_codes:
            resp.raise_for_status()
        return resp

    resp = _do_get()
    resp.raise_for_status()
    return resp.json()
