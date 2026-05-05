from __future__ import annotations

from app.config import get_settings
from app.tushare_client import TushareClient


def main() -> None:
    client = TushareClient(get_settings())
    stock_code = "000001.SZ"
    index_code = "000001.SH"
    trade_date = "20260505"
    start_date = "20260505"
    end_date = "20260505"
    finance_start = "20250101"
    finance_end = "20260505"

    cases = [
        ("trade_cal", {"exchange": "SSE", "start_date": start_date, "end_date": end_date}),
        ("daily", {"trade_date": trade_date}),
        ("daily_basic", {"trade_date": trade_date}),
        ("adj_factor", {"ts_code": stock_code, "start_date": start_date, "end_date": end_date}),
        ("index_basic", {"market": "SSE"}),
        ("index_daily", {"ts_code": index_code, "start_date": start_date, "end_date": end_date}),
        ("index_dailybasic", {"trade_date": trade_date}),
        ("moneyflow_ths", {"trade_date": trade_date}),
        ("kpl_concept_cons", {"trade_date": trade_date}),
        ("fina_indicator", {"ts_code": stock_code, "start_date": finance_start, "end_date": finance_end}),
        ("income", {"ts_code": stock_code, "start_date": finance_start, "end_date": finance_end}),
        ("balancesheet", {"ts_code": stock_code, "start_date": finance_start, "end_date": finance_end}),
        ("cashflow", {"ts_code": stock_code, "start_date": finance_start, "end_date": finance_end}),
    ]

    for api_name, params in cases:
        try:
            frame = client.query(api_name, **params)
            columns = ",".join(list(frame.columns[:6])) if len(frame.columns) else ""
            print(f"OK\t{api_name}\trows={len(frame)}\tcols={columns}")
        except Exception as exc:  # noqa: BLE001 - smoke test reports all failures
            message = str(exc).replace("\n", " ")[:300]
            print(f"ERR\t{api_name}\t{type(exc).__name__}\t{message}")


if __name__ == "__main__":
    main()
