"""External Reality Checker — Deep Research Agent for company analysis
beyond official company filings - suppliers, customers, controversies, etc.

Uses Business Overview and Narrative Validation reports as context so the
Deep Research agent can cross-reference its internet findings against the
company's own filings.
"""

import logging
import os

from helios.config import GEMINI, REASONING_AGENT_SPECS_DIR, OutputDir
from helios.utils.deep_research_analyser import DeepResearchAnalyser
from helios.utils.commons import format_dossier_section, read_dir_files

logger = logging.getLogger(__name__)


class ExternalRealityChecker(DeepResearchAnalyser):
    """External Reality checker via Gemini Deep Research Agent."""

    AGENT_SPEC_FILENAME = "external_reality_check.md"
    AGENT_SPECS_DIR = REASONING_AGENT_SPECS_DIR

    def _get_model(self) -> str:
        return GEMINI.external_reality_check_model

    def _build_output_path(self) -> str:
        year, quarter = self._current_quarter()
        return os.path.join(
            self._ticker_output_dir(OutputDir.EXTERNAL_REALITY_CHECK), f"report_during_{year}-Q{quarter}.md"
        )

    def _build_agent_spec(self) -> str:
        return self._render_agent_spec(self.AGENT_SPEC_FILENAME, TICKER=self.ticker)

    def _build_context(self) -> str:
        """Load Business Overview and Narrative Validation reports as context."""
        ticker_dir = os.path.join(self.output_base_dir, self.ticker)

        sections = [
            format_dossier_section(
                "BUSINESS OVERVIEW", read_dir_files(os.path.join(ticker_dir, OutputDir.BUSINESS_OVERVIEW), "*.md")
            ),
            format_dossier_section(
                "NARRATIVE VALIDATION",
                read_dir_files(os.path.join(ticker_dir, OutputDir.NARRATIVE_VALIDATION), "*.md"),
            ),
        ]

        dossier = "\n\n".join(s for s in sections if s)
        if not dossier:
            logger.warning("[ExternalRealityChecker] No BE/NV context found — running without upstream context.")
            return ""

        logger.info(f"[ExternalRealityChecker] Loaded BE + NV context ({len(dossier):,} chars).")
        return dossier
