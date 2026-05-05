from __future__ import annotations

import logging
from typing import Optional

import typer

from app.config import get_settings
from app.db import create_db_engine, init_schema
from app.sync.base import SyncContext, lookback_start, run_many, today_yyyymmdd
from app.sync.registry import DATASETS, datasets_for
from app.tushare_client import TushareClient


app = typer.Typer(help="Tushare data sync tool.")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def build_context() -> SyncContext:
    settings = get_settings()
    engine = create_db_engine(settings)
    client = TushareClient(settings)
    return SyncContext(client=client, engine=engine, settings=settings)


@app.command("init-db")
def init_db() -> None:
    """Create or update database tables."""
    settings = get_settings()
    engine = create_db_engine(settings)
    init_schema(engine)
    typer.echo("Database schema initialized.")


@app.command("list")
def list_datasets() -> None:
    """List supported sync datasets."""
    for name in sorted(DATASETS):
        typer.echo(name)
    typer.echo("all")
    typer.echo("daily_group")
    typer.echo("finance_group")


@app.command("sync")
def sync(
    dataset: str = typer.Argument(..., help="Dataset name, or all/daily_group/finance_group."),
    start_date: Optional[str] = typer.Option(None, "--start", help="Start date, YYYYMMDD."),
    end_date: Optional[str] = typer.Option(None, "--end", help="End date, YYYYMMDD."),
    ts_code: Optional[str] = typer.Option(None, "--ts-code", help="Single stock/index code."),
    history: bool = typer.Option(False, "--history", help="Mark this run as historical backfill."),
) -> None:
    """Sync one dataset or a dataset group."""
    ctx = build_context()
    init_schema(ctx.engine)

    end = end_date or today_yyyymmdd()
    start = start_date or lookback_start(ctx.settings.sync_lookback_days)
    mode = "history" if history else "daily"

    selected = datasets_for(dataset)
    logger.info("Sync start dataset=%s start=%s end=%s mode=%s", dataset, start, end, mode)
    results = run_many(ctx, selected, start, end, mode, ts_code)
    for name, (fetched, affected) in results.items():
        typer.echo(f"{name}: fetched={fetched}, affected={affected}")


@app.command("scheduler")
def scheduler() -> None:
    """Start the built-in scheduler."""
    from app.scheduler import start_scheduler

    start_scheduler()


if __name__ == "__main__":
    app()
