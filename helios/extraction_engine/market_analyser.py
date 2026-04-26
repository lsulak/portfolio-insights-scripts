"""Market analyser — managed agent for market-level analysis."""

import os

from helios.config import GEMINI, EXTRACTION_FORCE, OutputDir
from helios.pipeline.managed_agent import ManagedAgentAnalyser
from helios.utils.helpers import current_quarter


class MarketAnalyser(ManagedAgentAnalyser):
    """Market analysis via managed AI agent."""

    AGENT_SPEC_FILE = "market_analysis.md"

    def __init__(self, **kwargs):
        kwargs.update({"force_recompute": EXTRACTION_FORCE.market})
        super().__init__(**kwargs)

    def _get_model(self) -> str:
        return GEMINI.market_analysis_model

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        return os.path.join(self._ticker_output_dir(OutputDir.MARKET_ANALYSIS), f"report_during_{year}-Q{quarter}.md")
