"""Base analyser — shared infrastructure for all HELIOS pipeline analysers.

Provides the common ``__init__``, caching, output persistence, ticker-dir
helpers, and agent-spec rendering used by both the Deep Research and Dossier
analyser families.
"""

import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path

from google import genai

from helios.utils.commons import current_quarter, generate_agent_spec, load_agent_spec

logger = logging.getLogger(__name__)


class BaseAnalyser(ABC):
    """Common base for every HELIOS pipeline analyser.

    Subclasses must set the class attribute:
        AGENT_SPECS_DIR — directory containing agent spec Markdown files

    And implement:
        _get_model()          – which Gemini model to use
        _build_output_path()  – where to write the result
        run()                 – execute the analyser
    """

    AGENT_SPECS_DIR: Path

    def __init__(
        self, client: genai.Client, output_base_dir: str, ticker: str, force_resummarize: bool = False
    ) -> None:
        self.client = client
        self.output_base_dir = output_base_dir
        self.ticker = ticker
        self.force_resummarize = force_resummarize

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    @abstractmethod
    def _get_model(self) -> str:
        """Return the Gemini model identifier for this analyser."""

    @abstractmethod
    def _build_output_path(self) -> str:
        """Return the full filesystem path for the output report."""

    @abstractmethod
    async def run(self):
        """Execute the analyser."""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _current_quarter() -> tuple[str, int]:
        """Return ``(year_str, quarter_number)`` for the current date."""
        return current_quarter()

    def _ticker_dir(self) -> str:
        """Return ``<output_base_dir>/<ticker>``."""
        return os.path.join(self.output_base_dir, self.ticker)

    def _ticker_output_dir(self, subdir: str) -> str:
        """Return ``<output_base_dir>/<ticker>/<subdir>``."""
        return os.path.join(self._ticker_dir(), subdir)

    def _render_agent_spec(self, spec_filename: str, **keywords: str) -> str:
        """Load an agent spec by filename and render it with the given keywords.

        Convenience wrapper combining ``load_agent_spec`` + ``generate_agent_spec``.
        Looks up the file under ``self.AGENT_SPECS_DIR``.
        """
        template = load_agent_spec(self.AGENT_SPECS_DIR / spec_filename)
        return generate_agent_spec(template, **keywords)

    def _is_cached(self, output_path: str) -> bool:
        """Return True (and log) if the output already exists and re-run is not forced."""
        if not self.force_resummarize and os.path.exists(output_path):
            logger.info(f"[{type(self).__name__}] Output already exists at {output_path}. Skipping.")
            return True
        return False

    def _persist(self, output_path: str, content: str) -> None:
        """Write *content* to *output_path*, creating directories as needed."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"[{type(self).__name__}] Report written to '{output_path}'")
