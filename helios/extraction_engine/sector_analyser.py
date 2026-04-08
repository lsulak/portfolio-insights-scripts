"""Sector analyser — managed agent for sector-level analysis."""

import os

from helios.config import GEMINI, EXTRACTION_FORCE, OutputDir
from helios.pipeline.managed_agent import ManagedAgentAnalyser
from helios.utils.helpers import current_quarter


class SectorAnalyser(ManagedAgentAnalyser):
    """Sector analysis via managed AI agent."""

    AGENT_SPEC_FILE = "sector_analysis.md"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.force_recompute = self.force_recompute or EXTRACTION_FORCE.sector

    def _get_model(self) -> str:
        return GEMINI.sector_analysis_model

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        return os.path.join(self._ticker_output_dir(OutputDir.SECTOR_ANALYSIS), f"report_during_{year}-Q{quarter}.md")
