"""External Reality Checker — Deep Research Agent for company analysis
beyond official company filings - suppliers, customers, controversies, etc.

Uses Business Overview and Narrative Validation reports as context so the
Deep Research agent can cross-reference its internet findings against the
company's own filings.
"""

import logging
import os

from helios.config import GEMINI, REASONING_AGENT_SPECS_DIR, OutputDir
from helios.utils.commons import DeepResearchAnalyser, read_dir_files

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
        sections: list[str] = []

        be_docs = read_dir_files(os.path.join(ticker_dir, OutputDir.BUSINESS_OVERVIEW), "*.md")
        if be_docs:
            sections.append("=" * 60)
            sections.append("BUSINESS OVERVIEW")
            sections.append("=" * 60)
            for label, content in be_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        nv_docs = read_dir_files(os.path.join(ticker_dir, OutputDir.NARRATIVE_VALIDATION), "*.md")
        if nv_docs:
            sections.append("\n" + "=" * 60)
            sections.append("NARRATIVE VALIDATION")
            sections.append("=" * 60)
            for label, content in nv_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        if not sections:
            raise Exception("[ExternalRealityChecker] No BE/NV context found — running without upstream context.")

        logger.info(f"[ExternalRealityChecker] Loaded BE + NV context ({sum(len(s) for s in sections):,} chars).")
        return "\n".join(sections)
