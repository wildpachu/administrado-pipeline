"""src/utils/utils.py — Directory setup and logging configuration."""
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from config import PATH_RAW_OWN, PATH_RAW_MARKET, PATH_PROCESSED_OWN, PATH_PROCESSED_MARKET, PATH_LOGS

DIRS = [
    PATH_RAW_OWN,
    PATH_RAW_MARKET,
    PATH_PROCESSED_OWN,
    PATH_PROCESSED_MARKET,
    PATH_LOGS,
]


def setup_dirs() -> None:
    """Creates all required data and log directories if they do not exist."""
    for path in DIRS:
        Path(path).mkdir(parents=True, exist_ok=True)


def setup_logger() -> None:
    """Configures the root logger with file and console handlers.

    Uses a RotatingFileHandler (max 5 MB per file, 3 backups) to prevent log
    files from growing unbounded across daily pipeline runs. Safe to call
    multiple times — clears existing handlers first.
    """
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if root.handlers:
        root.handlers.clear()
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = RotatingFileHandler(
        f"{PATH_LOGS}/logs.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)
