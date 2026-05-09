from __future__ import annotations

from importlib import resources
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Engine
from sqlalchemy import MetaData, Table

from app.config import Settings


def create_db_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.mysql_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={"connect_timeout": settings.mysql_connect_timeout},
        future=True,
    )


def init_schema(engine: Engine) -> None:
    schema = resources.files("app.models").joinpath("schema.sql").read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in schema.split(";") if stmt.strip()]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


def read_trade_dates(engine: Engine, start_date: str, end_date: str) -> list[str]:
    sql = text(
        """
        SELECT cal_date
        FROM trade_cal
        WHERE cal_date BETWEEN :start_date AND :end_date
          AND is_open = 1
        ORDER BY cal_date
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"start_date": start_date, "end_date": end_date}).all()
    return [row[0] for row in rows]


def read_stock_codes(engine: Engine) -> list[str]:
    sql = text("SELECT ts_code FROM stock_basic ORDER BY ts_code")
    with engine.connect() as conn:
        rows = conn.execute(sql).all()
    return [row[0] for row in rows]


def upsert_dataframe(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
    unique_columns: Iterable[str],
    batch_size: int,
) -> int:
    if df.empty:
        return 0

    unique_set = set(unique_columns)
    clean_df = df.astype(object).where(pd.notnull(df), None)
    rows = clean_df.to_dict(orient="records")

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    table_columns = set(table.columns.keys())
    filtered_rows = []
    for row in rows:
        filtered = {key: value for key, value in row.items() if key in table_columns}
        for column in unique_set:
            if column in table_columns and filtered.get(column) is None:
                filtered[column] = ""
        filtered_rows.append(filtered)
    filtered_rows = [row for row in filtered_rows if row]
    if not filtered_rows:
        return 0

    affected = 0
    with engine.begin() as conn:
        for index in range(0, len(filtered_rows), batch_size):
            chunk = filtered_rows[index : index + batch_size]
            stmt = mysql_insert(table).values(chunk)
            update_columns = {
                column.name: stmt.inserted[column.name]
                for column in table.columns
                if column.name not in unique_set and column.name in chunk[0]
            }
            if update_columns:
                stmt = stmt.on_duplicate_key_update(**update_columns)
            else:
                stmt = stmt.prefix_with("IGNORE")
            result = conn.execute(stmt)
            affected += result.rowcount or 0
    return affected
