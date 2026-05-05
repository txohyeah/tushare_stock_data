from __future__ import annotations

from datetime import datetime
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

from app.cli import build_context
from app.db import init_schema
from app.sync.base import lookback_start, run_many, today_yyyymmdd
from app.sync.registry import DATASETS, FINANCE_ORDER

logger = logging.getLogger(__name__)


def _run_daily_market_data() -> None:
    ctx = build_context()
    init_schema(ctx.engine)
    start = lookback_start(ctx.settings.sync_lookback_days)
    end = today_yyyymmdd()
    datasets = [
        DATASETS[name]
        for name in (
            "trade_cal",
            "stock_basic",
            "daily",
            "daily_basic",
            "adj_factor",
            "index_basic",
            "index_daily",
            "index_daily_basic",
            "moneyflow_ths",
            "kpl_concept_cons",
        )
    ]
    run_many(ctx, datasets, start, end, "scheduled")


def _run_finance_updates() -> None:
    ctx = build_context()
    init_schema(ctx.engine)
    start = lookback_start(30)
    end = today_yyyymmdd()
    run_many(ctx, [DATASETS[name] for name in FINANCE_ORDER], start, end, "scheduled")


def start_scheduler() -> None:
    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(_run_daily_market_data, "cron", day_of_week="mon-fri", hour=18, minute=10, id="daily_market_data")
    scheduler.add_job(_run_finance_updates, "cron", day_of_week="mon-sun", hour=21, minute=30, id="finance_updates")
    scheduler.start()

    logger.info("Scheduler started at %s. Press Ctrl+C to stop.", datetime.now().isoformat(timespec="seconds"))
    try:
        while True:
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped.")
