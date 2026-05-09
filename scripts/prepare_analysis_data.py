from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import json
import logging
from typing import Iterable

from app.config import get_settings
from app.db import create_db_engine, init_schema
from app.providers import FallbackProvider
from app.sync.base import DEFAULT_INDEX_CODES, run_dataset, today_yyyymmdd
from app.sync.registry import get_dataset
from app.tushare_client import TushareClient


logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

PROFILES: dict[str, tuple[str, ...]] = {
    "technical": ("trade_cal", "daily", "adj_factor", "index_daily"),
    "valuation": ("trade_cal", "daily", "daily_basic", "index_daily", "fina_indicator"),
    "financial": ("trade_cal", "fina_indicator", "income", "balancesheet", "cashflow"),
    "full": (
        "trade_cal",
        "daily",
        "adj_factor",
        "daily_basic",
        "index_daily",
        "fina_indicator",
        "income",
        "balancesheet",
        "cashflow",
    ),
}

PER_STOCK_DATASETS = {"daily", "adj_factor", "daily_basic", "fina_indicator", "income", "balancesheet", "cashflow"}
INDEX_DATASETS = {"index_daily"}


def build_context(enable_fallback: bool):
    from app.sync.base import SyncContext

    settings = get_settings()
    engine = create_db_engine(settings)
    client = TushareClient(settings)
    fallback_provider = FallbackProvider(settings) if enable_fallback else None
    return SyncContext(
        client=client,
        engine=engine,
        settings=settings,
        fallback_provider=fallback_provider,
        enable_fallback=enable_fallback,
    )


def parse_csv(value: str | None, *, upper: bool = True) -> list[str]:
    if not value:
        return []
    items = [item.strip() for item in value.split(",") if item.strip()]
    if upper:
        return [item.upper() for item in items]
    return items


def date_years_ago(years: int, end_date: str) -> str:
    end = datetime.strptime(end_date, "%Y%m%d")
    return (end - timedelta(days=365 * years)).strftime("%Y%m%d")


def choose_datasets(profile: str, datasets: str | None) -> list[str]:
    if datasets:
        return parse_csv(datasets, upper=False)
    if profile not in PROFILES:
        known = ", ".join(sorted(PROFILES))
        raise ValueError(f"Unknown profile: {profile}. Known profiles: {known}")
    return list(PROFILES[profile])


def sync_dataset_safely(ctx, dataset_name: str, start_date: str, end_date: str, mode: str, codes: Iterable[str | None]) -> dict:
    details = []
    fetched_total = 0
    affected_total = 0
    errors = []
    dataset = get_dataset(dataset_name)
    for code in codes:
        try:
            fetched, affected = run_dataset(ctx, dataset, start_date, end_date, mode, code)
            fetched_total += fetched
            affected_total += affected
            details.append({"ts_code": code, "status": "success", "fetched": fetched, "affected": affected})
        except Exception as exc:  # noqa: BLE001 - report every missing piece as JSON
            errors.append({"ts_code": code, "error": str(exc)})
            details.append({"ts_code": code, "status": "failed", "error": str(exc)})

    status = "success" if not errors else "failed"
    return {
        "status": status,
        "fetched": fetched_total,
        "affected": affected_total,
        "details": details,
        "errors": errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare MySQL data required by stock-research analysis.")
    parser.add_argument("--ts-codes", default="", help="Comma separated stock codes, for example 600519.SH,000001.SZ.")
    parser.add_argument("--index-codes", default=",".join(DEFAULT_INDEX_CODES), help="Comma separated index codes.")
    parser.add_argument("--profile", default="technical", choices=sorted(PROFILES), help="Analysis data profile.")
    parser.add_argument("--datasets", default=None, help="Comma separated dataset override.")
    parser.add_argument("--start", default=None, help="Start date, YYYYMMDD.")
    parser.add_argument("--end", default=None, help="End date, YYYYMMDD.")
    parser.add_argument("--years", type=int, default=3, help="Lookback years when --start is omitted.")
    parser.add_argument("--history", action="store_true", help="Mark sync_run rows as history mode.")
    parser.add_argument("--no-fallback", action="store_true", help="Disable fallback data sources.")
    args = parser.parse_args()

    end_date = args.end or today_yyyymmdd()
    start_date = args.start or date_years_ago(args.years, end_date)
    ts_codes = parse_csv(args.ts_codes)
    index_codes = parse_csv(args.index_codes) or list(DEFAULT_INDEX_CODES)
    datasets = choose_datasets(args.profile, args.datasets)
    mode = "history" if args.history else "analysis_prepare"

    ctx = build_context(enable_fallback=not args.no_fallback)
    init_schema(ctx.engine)

    result = {
        "status": "success",
        "profile": args.profile,
        "datasets": datasets,
        "ts_codes": ts_codes,
        "index_codes": index_codes,
        "start_date": start_date,
        "end_date": end_date,
        "fallback_enabled": ctx.enable_fallback,
        "synced": {},
        "missing_or_failed": [],
        "warnings": [],
        "can_retry_stock_research": True,
    }

    for dataset_name in datasets:
        if dataset_name in PER_STOCK_DATASETS:
            if not ts_codes:
                result["synced"][dataset_name] = {"status": "skipped", "reason": "no ts_codes"}
                result["warnings"].append(f"{dataset_name} skipped because --ts-codes is empty")
                continue
            codes: Iterable[str | None] = ts_codes
        elif dataset_name in INDEX_DATASETS:
            codes = index_codes
        else:
            codes = [None]

        sync_result = sync_dataset_safely(ctx, dataset_name, start_date, end_date, mode, codes)
        result["synced"][dataset_name] = sync_result
        if sync_result["status"] != "success":
            result["missing_or_failed"].append(dataset_name)

    if result["missing_or_failed"]:
        result["status"] = "partial_success" if any(
            item.get("status") == "success" for item in result["synced"].values()
        ) else "failed"
    result["can_retry_stock_research"] = result["status"] in {"success", "partial_success"}

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
