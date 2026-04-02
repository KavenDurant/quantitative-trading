from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import pandas as pd
from scipy import stats

from qt.common.logger import get_logger

logger = get_logger(__name__)


class DecayStatus(Enum):
    HEALTHY = "healthy"
    WARNING = "warning"
    DECAYED = "decayed"


@dataclass(slots=True)
class DecayReport:
    factor_name: str
    status: DecayStatus
    current_ic: float
    historical_mean_ic: float
    historical_std_ic: float
    ir: float  # Information Ratio = mean_ic / std_ic
    trend_slope: float  # 趋势斜率（负值表示衰减）
    trend_pvalue: float  # 趋势显著性
    sample_size: int
    as_of_date: str
    message: str


def _compute_ic(
    factor_values: pd.Series,
    forward_returns: pd.Series,
) -> float:
    if factor_values.empty or forward_returns.empty:
        return 0.0
    aligned = pd.concat([factor_values, forward_returns], axis=1).dropna()
    if len(aligned) < 3:
        return 0.0
    corr, _ = stats.spearmanr(aligned.iloc[:, 0], aligned.iloc[:, 1])
    return float(corr) if not pd.isna(corr) else 0.0


def _compute_rank_ic(
    factor_values: pd.Series,
    forward_returns: pd.Series,
) -> float:
    if factor_values.empty or forward_returns.empty:
        return 0.0
    aligned = pd.concat([factor_values, forward_returns], axis=1).dropna()
    if len(aligned) < 3:
        return 0.0
    factor_ranks = aligned.iloc[:, 0].rank()
    return_ranks = aligned.iloc[:, 1].rank()
    corr, _ = stats.spearmanr(factor_ranks, return_ranks)
    return float(corr) if not pd.isna(corr) else 0.0


def _detect_trend(ic_series: pd.Series) -> tuple[float, float]:
    if len(ic_series) < 3:
        return 0.0, 1.0
    x = list(range(len(ic_series)))
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, ic_series.tolist())
    return float(slope), float(p_value)


def detect_factor_decay(
    factor_name: str,
    factor_history: pd.DataFrame,
    forward_returns_history: pd.DataFrame,
    lookback_months: int = 6,
    ic_threshold: float = 0.03,
    ir_threshold: float = 0.5,
    trend_pvalue_threshold: float = 0.1,
) -> DecayReport:
    """
    检测因子有效性衰减

    Args:
        factor_name: 因子名称（quality_score, value_score, expectation_score, composite_score）
        factor_history: 历史因子数据，需包含 trade_date 和因子列
        forward_returns_history: 历史未来收益数据，需包含 trade_date 和 forward_return 列
        lookback_months: 回看月数，用于计算历史均值/标准差
        ic_threshold: IC 阈值，低于此值触发警告
        ir_threshold: IR 阈值，低于此值且 IC 低时判定为衰减
        trend_pvalue_threshold: 趋势显著性阈值，p < 此值时认为趋势显著

    Returns:
        DecayReport: 包含检测结果和状态
    """
    as_of_date = datetime.now().strftime("%Y-%m-%d")

    if factor_name not in factor_history.columns:
        logger.warning("因子 %s 不在历史数据中", factor_name)
        return DecayReport(
            factor_name=factor_name,
            status=DecayStatus.WARNING,
            current_ic=0.0,
            historical_mean_ic=0.0,
            historical_std_ic=0.0,
            ir=0.0,
            trend_slope=0.0,
            trend_pvalue=1.0,
            sample_size=0,
            as_of_date=as_of_date,
            message=f"因子 {factor_name} 不在历史数据中",
        )

    merged = pd.merge(
        factor_history[["trade_date", factor_name]],
        forward_returns_history[["trade_date", "forward_return"]],
        on="trade_date",
        how="inner",
    ).sort_values("trade_date")

    if len(merged) < lookback_months * 20:  # 假设每月约20个交易日
        logger.warning("因子 %s 历史数据不足，样本量=%d", factor_name, len(merged))
        return DecayReport(
            factor_name=factor_name,
            status=DecayStatus.WARNING,
            current_ic=0.0,
            historical_mean_ic=0.0,
            historical_std_ic=0.0,
            ir=0.0,
            trend_slope=0.0,
            trend_pvalue=1.0,
            sample_size=len(merged),
            as_of_date=as_of_date,
            message=f"历史数据不足（需至少 {lookback_months * 20} 条，实际 {len(merged)}）",
        )

    recent = merged.tail(lookback_months * 20)

    ics = []
    for i in range(len(recent) - 1):
        factor_window = recent[factor_name].iloc[: i + 1]
        return_window = recent["forward_return"].iloc[1 : i + 2]
        if len(factor_window) == len(return_window) and len(factor_window) >= 3:
            ic = _compute_ic(factor_window.reset_index(drop=True), return_window.reset_index(drop=True))
            ics.append(ic)

    if not ics:
        return DecayReport(
            factor_name=factor_name,
            status=DecayStatus.WARNING,
            current_ic=0.0,
            historical_mean_ic=0.0,
            historical_std_ic=0.0,
            ir=0.0,
            trend_slope=0.0,
            trend_pvalue=1.0,
            sample_size=0,
            as_of_date=as_of_date,
            message="无法计算 IC 序列",
        )

    ic_series = pd.Series(ics)
    current_ic = ic_series.iloc[-1]
    mean_ic = ic_series.mean()
    std_ic = ic_series.std()
    ir = mean_ic / std_ic if std_ic > 0 else 0.0
    trend_slope, trend_pvalue = _detect_trend(ic_series)

    status = DecayStatus.HEALTHY
    message = "因子表现健康"

    if current_ic < ic_threshold and ir < ir_threshold:
        status = DecayStatus.DECAYED
        message = f"因子失效：当前 IC={current_ic:.4f} 低于阈值 {ic_threshold}，IR={ir:.4f} 低于阈值 {ir_threshold}"
    elif current_ic < ic_threshold:
        status = DecayStatus.WARNING
        message = f"因子警告：当前 IC={current_ic:.4f} 低于阈值 {ic_threshold}"
    elif trend_pvalue < trend_pvalue_threshold and trend_slope < -0.01:
        status = DecayStatus.WARNING
        message = f"因子衰减趋势显著：斜率={trend_slope:.4f} (p={trend_pvalue:.4f})"

    logger.info(
        "因子 %s 检测完成: status=%s current_ic=%.4f mean_ic=%.4f ir=%.4f slope=%.4f",
        factor_name,
        status.value,
        current_ic,
        mean_ic,
        ir,
        trend_slope,
    )

    return DecayReport(
        factor_name=factor_name,
        status=status,
        current_ic=current_ic,
        historical_mean_ic=mean_ic,
        historical_std_ic=std_ic,
        ir=ir,
        trend_slope=trend_slope,
        trend_pvalue=trend_pvalue,
        sample_size=len(merged),
        as_of_date=as_of_date,
        message=message,
    )


def batch_detect_decay(
    factor_names: list[str],
    factor_history: pd.DataFrame,
    forward_returns_history: pd.DataFrame,
    **kwargs,
) -> list[DecayReport]:
    """批量检测多个因子的衰减情况"""
    reports = []
    for name in factor_names:
        report = detect_factor_decay(name, factor_history, forward_returns_history, **kwargs)
        reports.append(report)
    return reports


def format_report(report: DecayReport) -> str:
    """格式化输出报告"""
    lines = [
        f"## 因子衰减检测报告 - {report.factor_name}",
        f"状态: {report.status.value.upper()}",
        f"检测日期: {report.as_of_date}",
        f"",
        f"IC 指标:",
        f"  当前 IC: {report.current_ic:.4f}",
        f"  历史 IC 均值: {report.historical_mean_ic:.4f}",
        f"  历史 IC 标准差: {report.historical_std_ic:.4f}",
        f"  IR (信息比率): {report.ir:.4f}",
        f"",
        f"趋势分析:",
        f"  趋势斜率: {report.trend_slope:.4f} (负值表示衰减)",
        f"  趋势显著性 p-value: {report.trend_pvalue:.4f}",
        f"",
        f"样本量: {report.sample_size}",
        f"",
        f"结论: {report.message}",
    ]
    return "\n".join(lines)
