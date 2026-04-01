from __future__ import annotations

from qt.common.logger import get_logger

logger = get_logger(__name__)


def audit_event(event: str, payload: dict[str, object]) -> None:
    logger.info("AUDIT %s %s", event, payload)
