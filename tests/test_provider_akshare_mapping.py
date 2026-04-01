from pathlib import Path

from qt.common.config import load_app_config


def test_load_app_config_reads_provider_and_backtest_window():
    project_root = Path("/Volumes/WD-1TB/WebstormProjects/quantitative-trading")
    config = load_app_config(project_root)

    assert config.data_provider == "baostock"
    assert config.backtest_start == "2025-01-01"
    assert config.backtest_end == "2025-12-31"
