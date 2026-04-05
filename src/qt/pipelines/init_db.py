from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

from qt.common.config import load_app_config
from qt.common.logger import get_logger
from qt.data.storage.sqlite_client import SQLiteClient

logger = get_logger(__name__)


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
    load_dotenv(project_root / ".env")
    config = load_app_config(project_root)
    client = SQLiteClient(config.db_path)
    client.init_db()
    logger.info("Initialized database schema at %s", config.db_path)


if __name__ == "__main__":
    main()
