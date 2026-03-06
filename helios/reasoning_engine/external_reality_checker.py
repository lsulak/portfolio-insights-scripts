"""External Reality Checker — Deep Research Agent for company analysis
beyond official company filings - suppliers, customers, controversies, etc.
"""

import os

from helios.config import GEMINI, REASONING_AGENT_SPECS_DIR, OutputDir
from helios.utils.commons import DeepResearchAnalyser


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
