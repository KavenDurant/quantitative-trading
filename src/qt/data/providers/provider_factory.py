from __future__ import annotations

from qt.data.providers.akshare_provider import AkshareProvider
from qt.data.providers.baostock_provider import BaostockProvider
from qt.data.providers.mock_provider import load_mock_dataset


class MockProvider:
    def load_dataset(self, as_of_date: str):
        return load_mock_dataset(as_of_date)

    def load_historical_dataset(self, start_date: str, end_date: str):
        return load_mock_dataset(end_date)


def get_provider(name: str):
    if name == "gm":
        try:
            from qt.data.providers.gm_provider import GmProvider
            return GmProvider()
        except ImportError:
            import warnings
            warnings.warn(
                "gm provider not available (requires 'goldminer' extra). "
                "Falling back to baostock.",
                RuntimeWarning,
            )
            return BaostockProvider()
    if name == "akshare":
        return AkshareProvider()
    if name == "baostock":
        return BaostockProvider()
    return MockProvider()


def get_best_available_provider():
    """自动选择最佳可用数据源: gm > baostock > akshare > mock"""
    try:
        import os
        if os.environ.get("GM_API_TOKEN"):
            from qt.data.providers.gm_provider import GmProvider
            return GmProvider()
    except ImportError:
        pass
    try:
        import baostock
        return BaostockProvider()
    except ImportError:
        pass
    try:
        import akshare
        return AkshareProvider()
    except ImportError:
        pass
    return MockProvider()
