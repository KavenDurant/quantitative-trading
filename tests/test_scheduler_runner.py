from datetime import datetime

from qt.pipelines import run_scheduler


def test_matches_cron_for_daily_weekday_evening():
    ts = datetime(2026, 4, 1, 18, 0)  # Wednesday
    assert run_scheduler._matches_cron("0 18 * * 1-5", ts)


def test_matches_cron_for_monthly_first_day():
    ts = datetime(2026, 4, 1, 9, 31)
    assert run_scheduler._matches_cron("31 9 1 * *", ts)


def test_run_once_daily_only(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(run_scheduler, "run_daily_checks_main", lambda: calls.append("daily"))
    monkeypatch.setattr(run_scheduler, "run_monthly_rebalance_main", lambda: calls.append("monthly"))

    run_scheduler._run_once("daily")

    assert calls == ["daily"]


def test_run_once_all(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(run_scheduler, "run_daily_checks_main", lambda: calls.append("daily"))
    monkeypatch.setattr(run_scheduler, "run_monthly_rebalance_main", lambda: calls.append("monthly"))

    run_scheduler._run_once("all")

    assert calls == ["daily", "monthly"]
