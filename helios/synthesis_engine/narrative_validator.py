"""Narrative Validator — cross-references extraction engine outputs to detect
narrative dissonance, misaligned incentives, and emerging asymmetrical risks.

Consumes outputs from the three extraction engine analysers:
    1. Sector Analysis    (Markdown)
    2. Earnings Calls     (Markdown)
    3. Edgar Filings      (JSON, grouped by form type, latest first)

and feeds them as a single compiled dossier to a Gemini model with the
``narrative_validator.md`` agent spec.

The result is a Markdown audit report.
"""

import logging
import os

from helios.config import GEMINI, SYNTHESIS_AGENT_SPECS_DIR, OutputDir
from helios.utils.commons import DossierAnalyser, current_quarter

logger = logging.getLogger(__name__)


class NarrativeValidator(DossierAnalyser):
    """Forensic cross-referencing of all extraction engine outputs for a ticker.

    Gathers sector analysis, earnings call synthesis, and summarized Edgar
    filings, then feeds them to a Gemini model with the narrative validator
    agent spec to produce a Markdown audit report.
    """

    AGENT_SPEC_FILE = "narrative_validator.md"
    AGENT_SPECS_DIR = SYNTHESIS_AGENT_SPECS_DIR

    def _get_model(self) -> str:
        return GEMINI.narrative_validator_model

    def _get_temperature(self) -> float:
        return GEMINI.narrative_validator_temperature

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        filename = f"report_{year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.NARRATIVE_VALIDATION, filename)

    def _compile_dossier(self) -> str:
        sections: list[str] = []
        self._add_dossier_section(sections, "SECTOR ANALYSIS", self._collect_files(OutputDir.SECTOR_ANALYSIS, "*.md"))
        self._add_dossier_section(
            sections, "EARNINGS CALL SYNTHESIS", self._collect_files(OutputDir.EARNINGS_CALLS, "*.md")
        )
        self._add_dossier_section(sections, "EDGAR FILINGS (latest first)", self._collect_edgar_filings())

        if not sections:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. " f"Run the extraction engine first."
            )
        return "\n".join(sections)
