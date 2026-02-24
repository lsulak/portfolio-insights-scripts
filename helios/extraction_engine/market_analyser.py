"""Market analyser — Deep Research Agent for market-level analysis."""

import os

from helios.config import GEMINI
from helios.utils.commons import DeepResearchAnalyser


class MarketAnalyser(DeepResearchAnalyser):
    """Market analysis via Gemini Deep Research Agent."""

    OUT_DIR_NAME = "market_analysis"
    AGENT_SPEC_FILENAME = "market_analysis.md"

    def _get_model(self) -> str:
        return GEMINI.market_analysis_model

    def _build_output_path(self) -> str:
        year, quarter = self._current_quarter()
        return os.path.join(self._ticker_output_dir(self.OUT_DIR_NAME), f"report_during_{year}Q{quarter}.md")

    def _build_agent_spec(self) -> str:
        return self._render_agent_spec(self.AGENT_SPEC_FILENAME, TICKER=self.ticker)
