from __future__ import annotations

from pathlib import Path

from qt.common.logger import get_logger
from qt.pipelines.run_backtest import run_backtest

logger = get_logger(__name__)


def run_optimization() -> None:
    project_root = Path(__file__).resolve().parents[3]

    # 第1轮：因子权重优化
    weight_schemes = {
        "A_质量35_预期40_估值25": {"quality": 0.35, "value": 0.25, "expectation": 0.40},
        "B_质量25_预期55_估值20": {"quality": 0.25, "value": 0.20, "expectation": 0.55},
        "C_质量33_预期34_估值33": {"quality": 0.33, "value": 0.33, "expectation": 0.34},
        "D_默认_质量40_估值35_预期25": {"quality": 0.40, "value": 0.35, "expectation": 0.25},
    }

    logger.info("=" * 60)
    logger.info("第1轮：因子权重优化")
    logger.info("=" * 60)
    weight_results = {}
    for name, weights in weight_schemes.items():
        report = run_backtest(project_root, factor_weights=weights)
        weight_results[name] = report
        logger.info("方案 %s: 总收益=%.4f 年化=%.4f 夏普=%.4f 最大回撤=%.4f",
                     name, report["total_return"], report["annualized_return"],
                     report["sharpe_ratio"], report["max_drawdown"])

    best_weight = max(weight_results, key=lambda k: weight_results[k]["sharpe_ratio"])
    best_weights = weight_schemes[best_weight]
    logger.info("最优权重方案: %s -> %s", best_weight, best_weights)

    # 第2轮：持仓数量优化
    position_counts = [3, 5, 8]
    logger.info("=" * 60)
    logger.info("第2轮：持仓数量优化 (使用最优权重)")
    logger.info("=" * 60)
    pos_results = {}
    for n in position_counts:
        report = run_backtest(project_root, factor_weights=best_weights, max_positions=n)
        pos_results[n] = report
        logger.info("持仓%d只: 总收益=%.4f 年化=%.4f 夏普=%.4f 最大回撤=%.4f",
                     n, report["total_return"], report["annualized_return"],
                     report["sharpe_ratio"], report["max_drawdown"])

    best_pos = max(pos_results, key=lambda k: pos_results[k]["sharpe_ratio"])
    logger.info("最优持仓数: %d", best_pos)

    # 第3轮：止损止盈优化
    stop_losses = [-0.05, -0.08, -0.10]
    logger.info("=" * 60)
    logger.info("第3轮：止损优化 (使用最优权重+持仓数)")
    logger.info("=" * 60)
    sl_results = {}
    for sl in stop_losses:
        report = run_backtest(project_root, factor_weights=best_weights, max_positions=best_pos, stop_loss_pct=sl)
        sl_results[sl] = report
        logger.info("止损%.0f%%: 总收益=%.4f 年化=%.4f 夏普=%.4f 最大回撤=%.4f",
                     sl * 100, report["total_return"], report["annualized_return"],
                     report["sharpe_ratio"], report["max_drawdown"])

    best_sl = max(sl_results, key=lambda k: sl_results[k]["sharpe_ratio"])
    logger.info("最优止损: %.0f%%", best_sl * 100)

    # 最终结果
    logger.info("=" * 60)
    logger.info("最终最优参数:")
    logger.info("  因子权重: %s", best_weights)
    logger.info("  持仓数量: %d", best_pos)
    logger.info("  止损比例: %.0f%%", best_sl * 100)
    logger.info("=" * 60)


def main() -> None:
    run_optimization()


if __name__ == "__main__":
    main()
