from __future__ import annotations

import time
import logging
from typing import Any

import pandas as pd

from app.config import Settings

logger = logging.getLogger(__name__)


class TushareClient:
    def __init__(self, settings: Settings) -> None:
        if not settings.tushare_token:
            raise RuntimeError("TUSHARE_TOKEN is required. Please set it in .env.")
        import tushare as ts

        self._pro = ts.pro_api(settings.tushare_token)
        self._retry_times = settings.sync_retry_times
        self._backoff_seconds = settings.sync_retry_backoff_seconds
        self._interval_seconds = settings.sync_request_interval_seconds

    def query(self, api_name: str, **params: Any) -> pd.DataFrame:
        cleaned_params = {key: value for key, value in params.items() if value not in (None, "")}
        last_error: Exception | None = None
        for attempt in range(1, self._retry_times + 1):
            try:
                time.sleep(self._interval_seconds)
                logger.debug("Query Tushare api=%s params=%s", api_name, cleaned_params)
                result = self._pro.query(api_name, **cleaned_params)
                if result is None:
                    return pd.DataFrame()
                return result
            except Exception as exc:  # noqa: BLE001 - surface Tushare error after retries
                last_error = exc
                error_text = str(exc)
                if any(marker in error_text for marker in ("没有接口", "访问权限", "无权限", "permission")):
                    break
                if attempt >= self._retry_times:
                    break
                sleep_seconds = self._backoff_seconds * attempt
                logger.warning(
                    "Tushare query failed api=%s attempt=%s/%s: %s. retry in %ss",
                    api_name,
                    attempt,
                    self._retry_times,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
        detail = f": {last_error}" if last_error else ""
        raise RuntimeError(f"Tushare query failed: {api_name}{detail}") from last_error
