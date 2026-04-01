from __future__ import annotations

from abc import ABC, abstractmethod


class BrokerBase(ABC):
    @abstractmethod
    def positions(self) -> dict[str, int]:
        raise NotImplementedError

    @abstractmethod
    def cash(self) -> float:
        raise NotImplementedError

    @abstractmethod
    def apply_orders(self, rebalance_id: str, targets: list[tuple[str, int, float]]) -> None:
        raise NotImplementedError
