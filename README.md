# Tushare Stock Data Sync

Python 3 同步器，用于从 Tushare Pro 拉取股票、指数、财务和复权数据，并幂等写入 MySQL。

## 功能

- 支持手动同步和应用内定时任务
- 支持日常增量和历史回补
- 使用 MySQL 唯一键 + `ON DUPLICATE KEY UPDATE` 防止重复数据
- 批量写入，避免逐行入库
- 每次同步写入 `sync_run`，便于追踪成功、失败、耗时和影响行数

## 初始化

安装依赖：

```bash
pip install -r requirements.txt
```

配置 `.env`：

```bash
cp .env.example .env
```

然后填写 `TUSHARE_TOKEN` 和 MySQL 连接信息。真实 `.env` 已被 `.gitignore` 忽略，不会提交。

初始化数据库表：

```bash
python -m app.cli init-db
```

## 手动同步

同步最近几天的日常数据：

```bash
python -m app.cli sync daily_group
```

同步单个数据集：

```bash
python -m app.cli sync daily --start 20260501 --end 20260505
python -m app.cli sync adj_factor --start 20260501 --end 20260505
```

历史回补：

```bash
python -m app.cli sync all --start 20100101 --end 20260505 --history
```

财务数据通常按股票循环同步。可以先同步股票列表：

```bash
python -m app.cli sync stock_basic
python -m app.cli sync finance_group --start 20250101 --end 20260505
```

同步单只股票：

```bash
python -m app.cli sync fina_indicator --ts-code 000001.SZ --start 20200101 --end 20260505
```

## 定时任务

启动内置定时器：

```bash
python -m app.cli scheduler
```

当前定时策略：

- 交易日 18:10 同步行情、每日指标、复权、指数和补充数据
- 每天 21:30 同步最近 30 天公告窗口内的财务数据

## 数据集

查看支持的数据集：

```bash
python -m app.cli list
```
