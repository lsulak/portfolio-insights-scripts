"""Pure utility helpers — dates, file I/O, text formatting, and logging setup."""

import logging
from logging.handlers import RotatingFileHandler
import os
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(log_dir: str) -> None:
    """Configure logging for the application."""
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "__main__.log")
    rotating_handler = RotatingFileHandler(log_file, maxBytes=1000 * 1024, backupCount=30)  # 1 MB

    console_handler = logging.StreamHandler(sys.stdout)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)",
        handlers=[rotating_handler, console_handler],
        force=True,
    )

    # Mute chatty third-party libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("google_genai.models").setLevel(logging.WARNING)


def current_quarter() -> tuple[str, int]:
    """Return ``(year_str, quarter_number)`` for the current date."""
    now = datetime.now()
    quarter = ((now.month - 1) // 3) + 1
    return str(now.year), quarter


def read_dir_files(directory: str, pattern: str) -> list[tuple[str, str]]:
    """Read all files matching *pattern* in *directory*, sorted descending by name.

    Raises:
        FileNotFoundError: If the directory does not exist.
        ValueError: If any matched file is empty.
    """
    if not os.path.isdir(directory):
        raise FileNotFoundError(f"Directory not found: {directory}")

    files = sorted(Path(directory).glob(pattern), reverse=True)
    results: list[tuple[str, str]] = []
    for fp in files:
        if fp.stat().st_size == 0:
            raise ValueError(f"Empty content in: {fp}.")
        results.append((fp.name, fp.read_text(encoding="utf-8")))
    return results


def format_section(heading: str, docs: list[tuple[str, str]]) -> str:
    """Format a labeled document group as a section string.

    Returns an empty string when *docs* is empty, allowing callers to
    filter out unused sections easily.
    """
    if not docs:
        return ""
    separator = "=" * 60
    parts = [separator, heading, separator]
    for label, content in docs:
        parts.append(f"\n--- {label} ---\n")
        parts.append(content)
    return "\n".join(parts)
