from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import Callable, Iterable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.config import Settings
from app.db import read_stock_codes, read_trade_dates, upsert_dataframe
from app.providers import FallbackProvider
from app.tushare_client import TushareClient

logger = logging.getLogger(__name__)

DEFAULT_INDEX_CODES = (
    "000001.SH",
    "399001.SZ",
    "399006.SZ",
    "000300.SH",
    "000905.SH",
    "000852.SH",
    "000688.SH",
)


SyncFunction = Callable[["SyncContext", "Dataset", str, str, str | None], tuple[int, int]]


@dataclass(frozen=True)
class Dataset:
    name: str
    api_name: str
    table_name: str
    unique_columns: tuple[str, ...]
    strategy: str
    default_params: dict[str, str] | None = None


@dataclass
class SyncContext:
    client: TushareClient
    engine: Engine
    settings: Settings
    fallback_provider: FallbackProvider | None = None
    enable_fallback: bool = True


def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def lookback_start(days: int) -> str:
    return (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")


def calendar_days(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    days = []
    current = start
    while current <= end:
        days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return days


def open_trade_days_or_calendar(engine: Engine, start_date: str, end_date: str) -> list[str]:
    trade_dates = read_trade_dates(engine, start_date, end_date)
    if trade_dates:
        return trade_dates
    return calendar_days(start_date, end_date)


def should_fallback(exc: Exception) -> bool:
    error_text = str(exc)
    return any(
        marker in error_text
        for marker in (
            "Tushare query failed",
            "没有接口",
            "访问权限",
            "无权限",
            "permission",
            "timeout",
            "timed out",
            "Connection",
        )
    )


def insert_sync_run(
    engine: Engine,
    dataset: str,
    mode: str,
    start_date: str,
    end_date: str,
) -> int:
    sql = text(
        """
        INSERT INTO sync_run (dataset, mode, start_date, end_date, status)
        VALUES (:dataset, :mode, :start_date, :end_date, 'running')
        """
    )
    with engine.begin() as conn:
        result = conn.execute(
            sql,
            {
                "dataset": dataset,
                "mode": mode,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        return int(result.lastrowid)


def finish_sync_run(
    engine: Engine,
    run_id: int,
    status: str,
    fetched_rows: int,
    affected_rows: int,
    error_message: str | None = None,
) -> None:
    sql = text(
        """
        UPDATE sync_run
        SET status = :status,
            finished_at = NOW(),
            fetched_rows = :fetched_rows,
            affected_rows = :affected_rows,
            error_message = :error_message
        WHERE id = :id
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "id": run_id,
                "status": status,
                "fetched_rows": fetched_rows,
                "affected_rows": affected_rows,
                "error_message": error_message,
            },
        )


def upsert(ctx: SyncContext, dataset: Dataset, frame: pd.DataFrame) -> int:
    return upsert_dataframe(
        ctx.engine,
        dataset.table_name,
        frame,
        dataset.unique_columns,
        ctx.settings.sync_batch_size,
    )


def upsert_with_source_log(ctx: SyncContext, dataset: Dataset, frame: pd.DataFrame, source: str, reason: str) -> int:
    affected = upsert(ctx, dataset, frame)
    logger.info(
        "%s fallback source=%s reason=%s fetched=%s affected=%s",
        dataset.name,
        source,
        reason,
        len(frame),
        affected,
    )
    return affected


def sync_single_call(ctx: SyncContext, dataset: Dataset, start_date: str, end_date: str, ts_code: str | None) -> tuple[int, int]:
    params = dict(dataset.default_params or {})
    if ts_code:
        params["ts_code"] = ts_code
    if dataset.strategy == "basic":
        frame = ctx.client.query(dataset.api_name, **params)
    else:
        frame = ctx.client.query(dataset.api_name, start_date=start_date, end_date=end_date, **params)
    return len(frame), upsert(ctx, dataset, frame)


def sync_by_trade_date(ctx: SyncContext, dataset: Dataset, start_date: str, end_date: str, ts_code: str | None) -> tuple[int, int]:
    fetched = 0
    affected = 0
    params = dict(dataset.default_params or {})
    if ts_code:
        params["ts_code"] = ts_code
    stock_codes: list[str] | None = None
    for trade_date in open_trade_days_or_calendar(ctx.engine, start_date, end_date):
        try:
            frame = ctx.client.query(dataset.api_name, trade_date=trade_date, **params)
        except Exception as exc:
            if dataset.name not in ("daily", "index_daily") or not ctx.enable_fallback or ctx.fallback_provider is None or not should_fallback(exc):
                raise
            if dataset.name == "daily" and stock_codes is None:
                stock_codes = read_stock_codes(ctx.engine)
            if dataset.name == "daily":
                fallback = ctx.fallback_provider.fetch_daily_by_trade_date(
                    trade_date=trade_date,
                    ts_code=ts_code,
                    stock_codes=stock_codes or [],
                    reason=str(exc),
                )
            else:
                fallback = ctx.fallback_provider.fetch_index_daily_by_trade_date(
                    trade_date=trade_date,
                    ts_code=ts_code,
                    index_codes=list(DEFAULT_INDEX_CODES),
                    reason=str(exc),
                )
            frame = fallback.frame
            fetched += len(frame)
            affected += upsert_with_source_log(ctx, dataset, frame, fallback.source, fallback.reason)
            logger.info("%s %s fetched=%s affected_total=%s", dataset.name, trade_date, len(frame), affected)
            continue
        if frame.empty and dataset.name in ("daily", "index_daily") and ctx.enable_fallback and ctx.fallback_provider is not None:
            if dataset.name == "daily" and stock_codes is None:
                stock_codes = read_stock_codes(ctx.engine)
            if dataset.name == "daily":
                fallback = ctx.fallback_provider.fetch_daily_by_trade_date(
                    trade_date=trade_date,
                    ts_code=ts_code,
                    stock_codes=stock_codes or [],
                    reason="tushare_empty",
                )
            else:
                fallback = ctx.fallback_provider.fetch_index_daily_by_trade_date(
                    trade_date=trade_date,
                    ts_code=ts_code,
                    index_codes=list(DEFAULT_INDEX_CODES),
                    reason="tushare_empty",
                )
            if not fallback.frame.empty:
                frame = fallback.frame
                fetched += len(frame)
                affected += upsert_with_source_log(ctx, dataset, frame, fallback.source, fallback.reason)
                logger.info("%s %s fetched=%s affected_total=%s", dataset.name, trade_date, len(frame), affected)
                continue
        fetched += len(frame)
        affected += upsert(ctx, dataset, frame)
        logger.info("%s %s fetched=%s affected=%s", dataset.name, trade_date, len(frame), affected)
    return fetched, affected


def sync_by_stock(ctx: SyncContext, dataset: Dataset, start_date: str, end_date: str, ts_code: str | None) -> tuple[int, int]:
    codes = [ts_code] if ts_code else read_stock_codes(ctx.engine)
    if not codes:
        raise RuntimeError("No stock codes found. Run stock_basic sync first or pass --ts-code.")

    fetched = 0
    affected = 0
    params = dict(dataset.default_params or {})
    for code in codes:
        frame = ctx.client.query(dataset.api_name, ts_code=code, start_date=start_date, end_date=end_date, **params)
        fetched += len(frame)
        affected += upsert(ctx, dataset, frame)
        logger.info("%s %s fetched=%s affected_total=%s", dataset.name, code, len(frame), affected)
    return fetched, affected


def sync_trade_cal(ctx: SyncContext, dataset: Dataset, start_date: str, end_date: str, ts_code: str | None) -> tuple[int, int]:
    del ts_code
    try:
        frame = ctx.client.query("trade_cal", exchange="SSE", start_date=start_date, end_date=end_date)
    except Exception as exc:
        if not ctx.enable_fallback or ctx.fallback_provider is None or not should_fallback(exc):
            raise
        fallback = ctx.fallback_provider.fetch_trade_cal(start_date, end_date, str(exc))
        return len(fallback.frame), upsert_with_source_log(ctx, dataset, fallback.frame, fallback.source, fallback.reason)
    if frame.empty and ctx.enable_fallback and ctx.fallback_provider is not None:
        fallback = ctx.fallback_provider.fetch_trade_cal(start_date, end_date, "tushare_empty")
        return len(fallback.frame), upsert_with_source_log(ctx, dataset, fallback.frame, fallback.source, fallback.reason)
    return len(frame), upsert(ctx, dataset, frame)


def sync_index_basic(ctx: SyncContext, dataset: Dataset, start_date: str, end_date: str, ts_code: str | None) -> tuple[int, int]:
    del start_date, end_date, ts_code
    fetched = 0
    affected = 0
    for market in ("SSE", "SZSE", "CSI", "CICC", "SW", "MSCI", "OTH"):
        frame = ctx.client.query(dataset.api_name, market=market)
        fetched += len(frame)
        affected += upsert(ctx, dataset, frame)
    return fetched, affected


STRATEGIES: dict[str, SyncFunction] = {
    "basic": sync_single_call,
    "date_range": sync_single_call,
    "trade_date": sync_by_trade_date,
    "stock": sync_by_stock,
    "trade_cal": sync_trade_cal,
    "index_basic": sync_index_basic,
}


def run_dataset(ctx: SyncContext, dataset: Dataset, start_date: str, end_date: str, mode: str, ts_code: str | None = None) -> tuple[int, int]:
    run_id = insert_sync_run(ctx.engine, dataset.name, mode, start_date, end_date)
    try:
        fetched, affected = STRATEGIES[dataset.strategy](ctx, dataset, start_date, end_date, ts_code)
    except Exception as exc:
        finish_sync_run(ctx.engine, run_id, "failed", 0, 0, str(exc))
        raise
    finish_sync_run(ctx.engine, run_id, "success", fetched, affected)
    return fetched, affected


def run_many(ctx: SyncContext, datasets: Iterable[Dataset], start_date: str, end_date: str, mode: str, ts_code: str | None = None) -> dict[str, tuple[int, int]]:
    results = {}
    for dataset in datasets:
        fetched, affected = run_dataset(ctx, dataset, start_date, end_date, mode, ts_code)
        results[dataset.name] = (fetched, affected)
    return results
