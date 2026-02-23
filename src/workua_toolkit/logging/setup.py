from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Optional


def setup_logging(
    *,
    logs_dir: str = "logs",
    level: int = logging.INFO,
    file_level: int = logging.DEBUG,
    filename: Optional[str] = None,
    max_bytes: int = 5_000_000,
    backup_count: int = 5,
) -> logging.Logger:
    """
    Configure root logging for the whole toolkit.

    - Console handler (level=level)
    - Rotating file handler in logs_dir (level=file_level)

    Safe to call multiple times (won't duplicate handlers).
    Returns the root logger.
    """
    os.makedirs(logs_dir, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(min(level, file_level))

    # Prevent duplicated handlers on repeated setup calls (e.g., Streamlit reruns)
    if getattr(root, "_workua_toolkit_configured", False):
        return root

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # File
    if filename is None:
        filename = f"workua_toolkit_{datetime.now().strftime('%Y-%m-%d')}.log"

    file_path = os.path.join(logs_dir, filename)
    fh = RotatingFileHandler(file_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    fh.setLevel(file_level)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    root._workua_toolkit_configured = True  # type: ignore[attr-defined]
    root.debug("Logging configured (console=%s, file=%s)", level, file_path)
    return root