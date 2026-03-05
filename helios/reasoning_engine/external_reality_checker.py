"""External Reality Checker — Deep Research Agent for company analysis 
beyond official company filings - suppliers, customers, controversies, etc.
"""

import os

from helios.config import GEMINI, REASONING_AGENT_SPECS_DIR, OutputDir
from helios.utils.commons import DeepResearchAnalyser, generate_agent_spec, load_agent_spec


class ExternalRealityChecker(DeepResearchAnalyser):
    """External Reality checker via Gemini Deep Research Agent."""

    AGENT_SPEC_FILENAME = "external_reality_check.md"

    def _get_model(self) -> str:
        return GEMINI.external_reality_check_model

    def _build_output_path(self) -> str:
        year, quarter = self._current_quarter()
        return os.path.join(self._ticker_output_dir(OutputDir.EXTERNAL_REALITY_CHECK), f"report_during_{year}-Q{quarter}.md")

    def _build_agent_spec(self) -> str:
        template = load_agent_spec(REASONING_AGENT_SPECS_DIR / self.AGENT_SPEC_FILENAME)
        return generate_agent_spec(template, TICKER=self.ticker)
