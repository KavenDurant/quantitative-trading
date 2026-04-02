from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from qt.common.config import load_schedule_config
from qt.common.logger import get_logger
from qt.pipelines.run_daily_checks import main as run_daily_checks_main
from qt.pipelines.run_monthly_rebalance import main as run_monthly_rebalance_main

logger = get_logger(__name__)

JobFunc = Callable[[], None]


def _parse_cron_field(field: str, value: int) -> bool:
    if field == "*":
        return True
    if field.startswith("*/"):
        step = int(field[2:])
        return value % step == 0
    if "-" in field:
        start, end = field.split("-", 1)
        return int(start) <= value <= int(end)
    if "," in field:
        return any(_parse_cron_field(part.strip(), value) for part in field.split(","))
    return int(field) == value


def _matches_cron(expression: str, now: datetime) -> bool:
    minute, hour, day, month, weekday = expression.split()
    cron_weekday = (now.weekday() + 1) % 7  # Python Mon=0...Sun=6 -> cron Sun=0
    return (
        _parse_cron_field(minute, now.minute)
        and _parse_cron_field(hour, now.hour)
        and _parse_cron_field(day, now.day)
        and _parse_cron_field(month, now.month)
        and _parse_cron_field(weekday, cron_weekday)
    )


def _run_job(job_name: str, job_func: JobFunc, tick: str) -> None:
    logger.info("触发任务 %s tick=%s", job_name, tick)
    job_func()


def _run_once(job: str) -> None:
    tick = datetime.now().replace(second=0, microsecond=0).isoformat()
    if job in ("daily", "all"):
        _run_job("run_daily_checks", run_daily_checks_main, tick)
    if job in ("monthly", "all"):
        _run_job("run_monthly_rebalance", run_monthly_rebalance_main, tick)


def _run_forever() -> None:
    project_root = Path(__file__).resolve().parents[3]
    schedule = load_schedule_config(project_root)

    logger.info(
        "调度器启动 daily_checks=%s monthly_rebalance=%s",
        schedule.daily_checks,
        schedule.monthly_rebalance,
    )

    last_daily_tick: str | None = None
    last_monthly_tick: str | None = None

    while True:
        now = datetime.now().replace(second=0, microsecond=0)
        tick = now.isoformat()

        if _matches_cron(schedule.daily_checks, now) and last_daily_tick != tick:
            _run_job("run_daily_checks", run_daily_checks_main, tick)
            last_daily_tick = tick

        if _matches_cron(schedule.monthly_rebalance, now) and last_monthly_tick != tick:
            _run_job("run_monthly_rebalance", run_monthly_rebalance_main, tick)
            last_monthly_tick = tick

        time.sleep(1)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="量化交易任务调度器")
    parser.add_argument(
        "--once",
        action="store_true",
        help="一次性执行任务并退出（用于验证）",
    )
    parser.add_argument(
        "--job",
        choices=["daily", "monthly", "all"],
        default="all",
        help="--once 模式下要执行的任务，默认 all",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    if args.once:
        _run_once(args.job)
        return
    _run_forever()


if __name__ == "__main__":
    main()
