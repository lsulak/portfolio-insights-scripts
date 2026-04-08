"""Base analyser — minimal shared infrastructure for all HELIOS pipeline analysers.

Provides only what is universally needed: ticker-scoped directory helpers,
output caching, and file persistence.  Has **no notion of AI, models, or
agent specs** — those concerns belong to the intermediate classes.

Class hierarchy::

    BaseAnalyser (ABC)          — pure I/O, caching, filesystem helpers
    ├── ManagedAgentAnalyser    — agentic approach (long-running agent polling)
    └── StatelessModelAnalyser  — direct LLM calls (file upload → generate)
"""

import asyncio
import inspect
import logging
import os
from abc import ABC, abstractmethod

from helios.utils.helpers import read_dir_files

logger = logging.getLogger(__name__)


class BaseAnalyser(ABC):
    """Common base for every HELIOS pipeline analyser.

    Holds only the bare minimum shared by *all* children:
    ticker-scoped directory resolution, output caching, file persistence,
    and upstream-file collection.

    Subclasses must implement:
        _build_output_path()  – where to write the result
        run()                 – execute the analyser
    """

    def __init__(self, output_base_dir: str, ticker: str, force_recompute: bool = False) -> None:
        self.output_base_dir = output_base_dir
        self.ticker = ticker
        self.force_recompute = force_recompute

    # ------------------------------------------------------------------
    # Abstract methods
    # ------------------------------------------------------------------

    @abstractmethod
    def _build_output_path(self) -> str:
        """Return the full filesystem path for the output report."""

    @abstractmethod
    def run(self) -> None:
        """Execute the analyser.  May be sync or async — the pipeline runner handles both."""

    # ------------------------------------------------------------------
    # Filesystem helpers
    # ------------------------------------------------------------------

    def _ticker_dir(self) -> str:
        """Return ``<output_base_dir>/<ticker>``."""
        return os.path.join(self.output_base_dir, self.ticker)

    def _ticker_output_dir(self, subdir: str) -> str:
        """Return ``<output_base_dir>/<ticker>/<subdir>``."""
        return os.path.join(self._ticker_dir(), subdir)

    def _collect_files(self, output_subdir: str, pattern: str) -> list[tuple[str, str]]:
        """Read all files matching *pattern* from ``<ticker_dir>/<output_subdir>``."""
        directory = os.path.join(self._ticker_dir(), output_subdir)
        return read_dir_files(directory, pattern)

    def _join_sections(self, sections: list[str]) -> str:
        """Join non-empty sections. Raises if all sections are empty."""
        combined = "\n\n".join(s for s in sections if s)
        if not combined:
            raise FileNotFoundError(f"No source documents found for {self.ticker}. Run prior pipeline stages first.")
        return combined

    # ------------------------------------------------------------------
    # Caching & persistence
    # ------------------------------------------------------------------

    def _is_cached(self, output_path: str) -> bool:
        """Return True (and log) if the output already exists and re-run is not forced."""
        if not self.force_recompute and os.path.exists(output_path):
            logger.info(f"[{type(self).__name__}] Output already exists at {output_path}. Skipping.")
            return True
        return False

    def _persist(self, output_path: str, content: str) -> None:
        """Write *content* to *output_path*, creating directories as needed."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"[{type(self).__name__}] Report written to '{output_path}'")

    # ------------------------------------------------------------------
    # Layer execution
    # ------------------------------------------------------------------

    @staticmethod
    async def run_layer(label: str, *analysers: "BaseAnalyser") -> None:
        """Run all analysers in a layer concurrently.  Raises on failure."""
        logger.info(f"═══ {label} ═══")

        async def _invoke(analyser: "BaseAnalyser") -> None:
            result = analyser.run()
            if inspect.isawaitable(result):
                await result

        results = await asyncio.gather(*(_invoke(a) for a in analysers), return_exceptions=True)

        failures = {type(a).__name__: exc for a, exc in zip(analysers, results) if isinstance(exc, BaseException)}
        if failures:
            for name, exc in failures.items():
                logger.error("❌ %s failed: %s: %s", name, type(exc).__name__, exc)
            raise RuntimeError(f"Analyser(s) failed: {', '.join(failures)}")
