# LLM Stock Analysis Data Skill

## When To Use

Use this workflow when a user asks for A-share stock analysis and `stock-research` reports missing data.

Call `stock-research` first. Only call this project after `stock-research` returns a structured `missing_data` result or asks for specific datasets.

## Responsibility Split

- `stock-research` reads MySQL and performs deterministic calculations.
- `tushare_stock_data` fills missing MySQL data.
- The LLM writes the final analysis report from `stock-research` results.

Do not ask the LLM to calculate indicators from raw K-line data unless `stock-research` is unavailable.

## Data Preparation Command

Run from the `tushare_stock_data` project root:

```bash
python -m scripts.prepare_analysis_data --ts-codes 600519.SH,000001.SZ --profile technical --years 3
```

When `stock-research` provides explicit required datasets:

```bash
python -m scripts.prepare_analysis_data --ts-codes 600519.SH --datasets daily,adj_factor,index_daily --start 20230101 --end 20260509
```

## Profiles

`technical` prepares:

- `trade_cal`
- `daily`
- `adj_factor`
- `index_daily`

`valuation` prepares:

- `trade_cal`
- `daily`
- `daily_basic`
- `index_daily`
- `fina_indicator`

`financial` prepares:

- `trade_cal`
- `fina_indicator`
- `income`
- `balancesheet`
- `cashflow`

`full` is for explicit deep analysis requests only. Do not use it by default.

## Output Contract

`scripts.prepare_analysis_data` writes data to MySQL and prints JSON status only. It does not print raw market data.

Check these fields:

- `status`: `success`, `partial_success`, or `failed`
- `synced`: per-dataset sync result
- `missing_or_failed`: datasets that could not be prepared
- `can_retry_stock_research`: whether to call `stock-research` again

If `can_retry_stock_research` is true, call `stock-research` again. If `missing_or_failed` is not empty, mention data gaps in the final report.

## Fallback Coverage

Currently implemented:

- `trade_cal`: `exchange_calendars`
- `daily`: Eastmoney historical K-line
- `index_daily`: Eastmoney historical K-line

Other datasets may fail when Tushare permissions are insufficient. Do not force full-market sync to compensate.

## Do Not

- Do not run full-market sync unless the user explicitly asks for it.
- Do not call `full` profile for a normal technical analysis request.
- Do not retry endlessly when a dataset is unsupported.
- Do not treat script output as analysis data; it is only preparation status.
