# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

A股基本面量化交易系统 MVP。沪深主板股票池，三因子（质量/估值/预期）月度调仓策略，最多持有 5 只，1 万元本金。SQLite + Streamlit 技术栈。

## 常用命令

```bash
# 安装依赖
pip install -e ".[dev]"

# 运行测试（pytest 已配置 pythonpath=src, testpaths=tests）
pytest                          # 全部测试
pytest tests/test_integration.py -v  # 单个测试文件
pytest tests/ -k "test_factor" -v    # 按名称匹配

# 代码检查
ruff check src/ tests/

# 数据库初始化
PYTHONPATH=src python -m qt.pipelines.init_db

# 数据回填（耗时数小时，需要 baostock 网络）
PYTHONPATH=src python -u -m qt.pipelines.backfill_data

# 仅刷新基本面数据（修复后的 ROE/增速等，约 10 分钟）
PYTHONPATH=src python -u scripts/refresh_fundamentals.py

# 运行回测
PYTHONPATH=src python -m qt.pipelines.run_backtest

# 启动调度器（模拟盘常驻进程）
PYTHONPATH=src python -u -m qt.pipelines.run_scheduler              # 守护模式
PYTHONPATH=src python -u -m qt.pipelines.run_scheduler --once --job all  # 单次执行

# 启动看板
streamlit run src/qt/monitoring/dashboard_app.py --server.port 8502

# 月度调仓（独立执行）
PYTHONPATH=src python -m qt.pipelines.run_monthly_rebalance

# 每日数据更新 + 质量检查
PYTHONPATH=src python -m qt.pipelines.run_daily_checks
```

## 架构

```
数据层 (data/)
  providers/     → baostock / gm / akshare / mock 四种数据源，通过 provider_factory 切换
  storage/       → SQLite 持久化，schema.py 建表，repository.py 读写，sqlite_client.py 连接管理
  ingest/        → universe_builder.py 股票池过滤（主板/非ST/价格区间/流动性）
  quality_check.py → 数据质量检查（数量/日期/分布）

因子层 (factors/)
  quality.py     → ROE + 毛利率 + 现金流比 → percentile_rank
  value.py       → PE/PB/PS (逆序) + 股息率 → percentile_rank
  expectation.py → 净利润增速 + 营收增速 + EPS修正 + 业绩惊喜 → percentile_rank
  combiner.py    → 三因子加权合成 (默认 quality:0.4 value:0.35 expectation:0.25)
  ml_composer.py → LightGBM 替代等权合成的 ML 分支
  normalize.py   → percentile_rank 核心函数
  decay_detector.py → 因子有效性 IC/IR 监控

策略层 (strategy/)
  selector.py         → 按 composite_score 选 top N
  position_sizer.py   → 等权分配 + 100股整手 + 现金缓冲
  rebalancer.py       → 目标 vs 当前仓位 → 交易信号
  risk_controls.py    → 止损/止盈/持仓期/集中度/组合止损/大盘择时

执行层 (execution/)
  paper_broker.py     → 模拟撮合（手续费+滑点）
  goldminer_broker.py → 掘金量化 API 实盘接口
  order_manager.py    → 订单执行器
  trading_engine.py   → 调仓+风控+收盘检查 完整引擎

监控层 (monitoring/)
  dashboard_app.py    → Streamlit 看板（净值曲线/持仓/交易/风控参数）
  notifier.py         → Server酱微信推送

流水线 (pipelines/)
  run_scheduler.py            → cron 调度器（每日检查 + 月度调仓）
  run_daily_checks.py         → 每日数据更新 + 因子衰减检测
  run_monthly_rebalance.py    → 月度调仓独立执行
  run_backtest.py             → 历史回测
  run_optimization.py         → 参数优化
  backfill_data.py            → 全量历史数据回填
  init_db.py                  → 建表

报告层 (reports/)
  monthly_pdf.py → 月度 PDF 报告生成
```

## 关键数据流

1. **数据拉取**: provider (baostock/gm) → `FundamentalSnapshot` / `DailyPrice` → SQLite
2. **因子计算**: `load_latest_fundamentals()` + `load_prices_for_date()` → `build_composite_scores()` → ranked DataFrame
3. **选股建仓**: `select_top_candidates()` → `assign_target_shares()` → `build_rebalance_signals()` → `OrderManager.execute()`
4. **调度**: `run_scheduler.py` 解析 cron 表达式，触发 daily_checks 和 monthly_rebalance

## baostock 字段映射（已验证正确，勿改）

```
profit_data: row[3]=roeAvg, row[5]=gpMargin, row[6]=netProfit(绝对值,非同比)
growth_data: row[5]=YOYNI(净利润同比), row[7]=YOYPNI(归属净利润同比)
cashflow_data: row[8]=CFOToNP(经营现金流/净利润)
```

季度计算用 `_latest_reported_quarter()`，基于 A 股财报披露截止日（Q1→4/30, Q2→8/31, Q3→10/31, Q4→次年4/30），确保只查已发布的季度。

## 配置

- `config/strategy.yaml` — 资金、持仓数、因子权重、止损止盈参数
- `config/data_sources.yaml` — 数据源选择和股票池规则
- `config/schedule.yaml` — cron 调度时间
- `.env` — GM_API_TOKEN, GM_ACCOUNT_ID, SERVERCHAN_SENDKEY 等密钥

## 项目规则

请始终参考 `TODO.md` 文件了解当前进度和下一步任务。

每完成一个任务后：
1. 在 TODO.md 中将该任务标记为 ✅
2. 自动继续执行下一个未完成的任务
3. 如果遇到阻塞，记录问题并跳到下一个可执行任务

每完成 TODO.md 中的一个任务：
1. git add 相关文件
2. git commit -m "feat: 完成任务 X.X - 任务描述"
3. 更新 TODO.md
4. git add TODO.md && git commit -m "docs: 更新进度"

### 执行模式
- 自主决策，不要中途停下来询问用户
- 遇到歧义时选择最通用的方案，并用代码注释标记 `# TODO: 需确认`
- 遇到报错先自行排查和修复，最多重试 3 次，修不了记录到 TODO.md 的阻塞项并跳过
- 零交互：不问问题、不要确认、不做中间汇报

### 上下文管理
当对话过长时，主动使用 /compact 压缩上下文。
压缩摘要中必须包含：当前 TODO.md 进度、正在进行的任务编号。
