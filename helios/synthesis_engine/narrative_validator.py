"""Narrative Validator — cross-references extraction engine outputs to detect
narrative dissonance, misaligned incentives, and emerging asymmetrical risks.

Consumes outputs from the three extraction engine analysers:
    1. Sector Analysis    (Markdown)
    2. Earnings Calls     (Markdown)
    3. Edgar Filings      (JSON, grouped by form type, latest first)

and feeds them as a single compiled document to a stateless model call with the
``narrative_validator.md`` agent spec.

The result is a Markdown audit report.
"""

import logging
import os

from helios.config import GEMINI, OutputDir
from helios.extraction_engine.edgar.reader import collect_edgar_filings
from helios.pipeline.stateless_model import StatelessModelAnalyser
from helios.utils.helpers import current_quarter, format_section

logger = logging.getLogger(__name__)


class NarrativeValidator(StatelessModelAnalyser):
    """Forensic cross-referencing of all extraction engine outputs for a ticker.

    Gathers sector analysis, earnings call synthesis, and summarized Edgar
    filings, then feeds them to a Gemini model with the narrative validator
    agent spec to produce a Markdown audit report.
    """

    AGENT_SPEC_FILE = "narrative_validator.md"

    def _get_model(self) -> str:
        return GEMINI.narrative_validator_model

    def _get_temperature(self) -> float:
        return GEMINI.narrative_validator_temperature

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        filename = f"report_{year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.NARRATIVE_VALIDATION, filename)

    def _compile_sources(self) -> str:
        sections = [
            format_section("SE: SECTOR ANALYSIS", self._collect_files(OutputDir.SECTOR_ANALYSIS, "*.md")),
            format_section("ETE: EARNINGS CALL SYNTHESIS", self._collect_files(OutputDir.EARNINGS_CALLS, "*.md")),
            format_section("EE: EDGAR FILINGS (latest first)", collect_edgar_filings(self._ticker_dir())),
        ]
        return self._join_sections(sections)
