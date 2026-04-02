from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from qt.common.logger import get_logger
from qt.data.storage.repository import Repository
from qt.data.storage.sqlite_client import SQLiteClient

logger = get_logger(__name__)


@dataclass(slots=True)
class MonthlyMetrics:
    month: str
    total_return: float
    max_drawdown: float
    sharpe_ratio: float
    monthly_win_rate: float
    start_nav: float
    end_nav: float
    num_trades: int
    num_positions: int


class MonthlyPDFReport:
    def __init__(self, db_path: Path, output_dir: Path):
        self.db_path = db_path
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _compute_monthly_metrics(
        self,
        nav_df: pd.DataFrame,
        month: str,
    ) -> MonthlyMetrics:
        from qt.backtest.metrics import (
            compute_annualized_return,
            compute_max_drawdown,
            compute_monthly_returns,
            compute_monthly_win_rate,
            compute_sharpe_ratio,
        )

        if nav_df.empty:
            return MonthlyMetrics(
                month=month,
                total_return=0.0,
                max_drawdown=0.0,
                sharpe_ratio=0.0,
                monthly_win_rate=0.0,
                start_nav=0.0,
                end_nav=0.0,
                num_trades=0,
                num_positions=0,
            )

        monthly_nav = nav_df[nav_df["trade_date"].str.startswith(month)]
        if monthly_nav.empty:
            monthly_nav = nav_df.tail(len(nav_df) // 2)

        start_nav = float(monthly_nav.iloc[0]["nav"])
        end_nav = float(monthly_nav.iloc[-1]["nav"])
        total_return = (end_nav - start_nav) / start_nav if start_nav > 0 else 0.0

        nav_series = monthly_nav["nav"]
        max_dd = compute_max_drawdown(nav_series)

        monthly_returns = compute_monthly_returns(nav_series)
        sharpe = compute_sharpe_ratio(monthly_returns) if not monthly_returns.empty else 0.0
        win_rate = compute_monthly_win_rate(monthly_returns) if not monthly_returns.empty else 0.0

        return MonthlyMetrics(
            month=month,
            total_return=total_return,
            max_drawdown=max_dd,
            sharpe_ratio=sharpe,
            monthly_win_rate=win_rate,
            start_nav=start_nav,
            end_nav=end_nav,
            num_trades=0,
            num_positions=0,
        )

    def _build_text_report(self, metrics: MonthlyMetrics, positions: pd.DataFrame, trades: pd.DataFrame) -> str:
        lines = [
            f"# A股基本面量化交易 - 月度报告",
            f"",
            f"报告月份: {metrics.month}",
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"",
            f"## 核心指标",
            f"",
            f"| 指标 | 数值 |",
            f"|------|------|",
            f"| 月初净值 | {metrics.start_nav:.2f} |",
            f"| 月末净值 | {metrics.end_nav:.2f} |",
            f"| 月度收益率 | {metrics.total_return:.2%} |",
            f"| 最大回撤 | {metrics.max_drawdown:.2%} |",
            f"| 夏普比率 | {metrics.sharpe_ratio:.4f} |",
            f"| 月度胜率 | {metrics.monthly_win_rate:.2%} |",
            f"",
        ]

        if not positions.empty:
            lines.extend([
                f"## 月末持仓",
                f"",
                f"| 代码 | 名称 | 持仓数 | 成本价 | 当前价 | 市值 |",
                f"|------|------|--------|--------|--------|------|",
            ])
            for _, row in positions.head(10).iterrows():
                code = row.get("code", "")
                shares = int(row.get("shares", 0))
                price = float(row.get("price", 0))
                market_val = shares * price
                lines.append(f"| {code} |  | {shares} |  | {price:.2f} | {market_val:.2f} |")

        if not trades.empty:
            lines.extend([
                f"",
                f"## 月度交易",
                f"",
                f"| 日期 | 代码 | 方向 | 数量 | 价格 | 金额 |",
                f"|------|------|------|------|------|------|",
            ])
            for _, row in trades.head(20).iterrows():
                date = row.get("trade_date", "")
                code = row.get("code", "")
                side = row.get("side", "")
                shares = int(row.get("shares", 0))
                price = float(row.get("price", 0))
                amount = float(row.get("amount", 0))
                lines.append(f"| {date} | {code} | {side} | {shares} | {price:.2f} | {amount:.2f} |")

        lines.extend([
            f"",
            f"---",
            f"*本报告由量化交易系统自动生成*",
        ])

        return "\n".join(lines)

    def generate(self, month: str | None = None) -> Path:
        client = SQLiteClient(self.db_path)
        with client.connect() as connection:
            repository = Repository(connection)

            nav_df = repository.load_latest_backtest_nav()
            positions_df = repository.load_latest_backtest_positions()
            trades_df = repository.load_latest_backtest_trades()

        if month is None:
            month = datetime.now().strftime("%Y-%m")

        metrics = self._compute_monthly_metrics(nav_df, month)
        metrics.num_positions = len(positions_df)
        metrics.num_trades = len(trades_df)

        text_report = self._build_text_report(metrics, positions_df, trades_df)

        report_path = self.output_dir / f"monthly_report_{month}.txt"
        report_path.write_text(text_report, encoding="utf-8")

        logger.info("月度报告已生成: %s", report_path)

        try:
            pdf_path = self._convert_to_pdf(text_report, month)
            return pdf_path
        except ImportError:
            logger.warning("reportlab 未安装，仅生成文本报告")
            return report_path

    def _convert_to_pdf(self, text_content: str, month: str) -> Path:
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
            from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
            from reportlab.lib import colors
        except ImportError:
            raise ImportError("reportlab 未安装，请运行: pip install reportlab")

        pdf_path = self.output_dir / f"monthly_report_{month}.pdf"
        doc = SimpleDocTemplate(str(pdf_path), pagesize=A4)
        styles = getSampleStyleSheet()
        story = []

        title_style = ParagraphStyle(
            "CustomTitle",
            parent=styles["Heading1"],
            fontSize=18,
            textColor=colors.HexColor("#1a1a1a"),
            spaceAfter=20,
            alignment=TA_CENTER,
        )

        lines = text_content.split("\n")
        i = 0
        table_data = []
        in_table = False
        table_headers = []

        while i < len(lines):
            line = lines[i].strip()

            if not line:
                if table_data:
                    table = Table(table_data, repeatRows=1)
                    table.setStyle(TableStyle([
                        ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, 0), 10),
                        ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                        ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                        ("GRID", (0, 0), (-1, -1), 1, colors.black),
                    ]))
                    story.append(table)
                    story.append(Spacer(1, 12))
                    table_data = []
                i += 1
                continue

            if line.startswith("# "):
                if table_data:
                    table = Table(table_data, repeatRows=1)
                    table.setStyle(TableStyle([
                        ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, 0), 10),
                        ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                        ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                        ("GRID", (0, 0), (-1, -1), 1, colors.black),
                    ]))
                    story.append(table)
                    story.append(Spacer(1, 12))
                    table_data = []

                title = line[2:].strip()
                story.append(Paragraph(title, title_style))
                story.append(Spacer(1, 12))

            elif line.startswith("|"):
                cells = [cell.strip() for cell in line.split("|")[1:-1]]
                if not cells:
                    i += 1
                    continue

                if line.startswith("|--"):
                    in_table = True
                    i += 1
                    continue

                table_data.append(cells)

            elif line.startswith("*") and line.endswith("*"):
                story.append(Paragraph(line[1:-1], styles["Italic"]))

            else:
                if table_data:
                    table = Table(table_data, repeatRows=1)
                    table.setStyle(TableStyle([
                        ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, 0), 10),
                        ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                        ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                        ("GRID", (0, 0), (-1, -1), 1, colors.black),
                    ]))
                    story.append(table)
                    story.append(Spacer(1, 12))
                    table_data = []

                story.append(Paragraph(line, styles["BodyText"]))

            i += 1

        if table_data:
            table = Table(table_data, repeatRows=1)
            table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ]))
            story.append(table)

        doc.build(story)
        logger.info("PDF 报告已生成: %s", pdf_path)
        return pdf_path


def generate_monthly_report(
    project_root: Path,
    month: str | None = None,
) -> Path:
    """生成月度报告的便捷函数"""
    from qt.common.config import load_app_config

    config = load_app_config(project_root)
    output_dir = project_root / "output" / "reports"

    reporter = MonthlyPDFReport(config.db_path, output_dir)
    return reporter.generate(month)
