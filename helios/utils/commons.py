"""General-purpose utilities shared across the HELIOS pipeline.

This module contains small, dependency-light helpers:
    - Agent spec loading / rendering (Jinja2)
    - Date helpers
    - File I/O helpers
    - Dossier section formatting
    - ResponseTypes enum

Gemini platform infrastructure lives in ``helios.utils.gemini_client``.
Abstract analyser base classes live in ``helios.utils.base_analysers``.
"""

import enum
import logging
import os
from datetime import datetime
from pathlib import Path

from jinja2 import Template

logger = logging.getLogger(__name__)


class ResponseTypes(enum.Enum):
    """Enum for expected response formats from Gemini agents."""

    JSON = "application/json"
    TEXT = "text/plain"


# ------------------------------------------------------------------
# Agent spec helpers
# ------------------------------------------------------------------


def load_agent_spec(spec_path: Path | str) -> Template:
    """Load a Markdown agent spec and compile it into a Jinja2 template.

    The Markdown file is the prompt — no intermediate parsing needed.
    Jinja2 variables (e.g. {{ business_and_risk }}) are resolved at render time.
    """
    logger.debug(f"Loading agent spec from {spec_path}...")

    with open(spec_path, "r", encoding="utf-8") as f:
        return Template(f.read())


def generate_agent_spec(spec_template: Template, **substitution_keywords: str) -> str:
    """Render a Jinja2 agent spec template with the given variables."""
    return spec_template.render(**substitution_keywords)


# ------------------------------------------------------------------
# General-purpose utilities
# ------------------------------------------------------------------


def current_quarter() -> tuple[str, int]:
    """Return ``(year_str, quarter_number)`` for the current date."""
    now = datetime.now()
    quarter = ((now.month - 1) // 3) + 1
    return str(now.year), quarter


def read_dir_files(directory: str, pattern: str) -> list[tuple[str, str]]:
    """Read all files matching *pattern* in *directory*, sorted descending by name.

    Raises:
        Exception: If the directory does not exist or any matched file is empty.
    """
    if not os.path.isdir(directory):
        raise Exception(f"Directory not found: {directory}")

    files = sorted(Path(directory).glob(pattern), reverse=True)
    results: list[tuple[str, str]] = []
    for fp in files:
        if fp.stat().st_size == 0:
            raise Exception(f"Empty content in: {fp}.")
        results.append((fp.name, fp.read_text(encoding="utf-8")))
    return results


def format_dossier_section(heading: str, docs: list[tuple[str, str]]) -> str:
    """Format a labeled document group as a dossier section string.

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
