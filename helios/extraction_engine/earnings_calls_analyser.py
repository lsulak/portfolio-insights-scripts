"""Earnings Call analyser — managed agent for earnings transcripts."""

import os
from datetime import datetime

from helios.config import GEMINI, EDGAR, EXTRACTION_FORCE, OutputDir
from helios.pipeline.managed_agent import ManagedAgentAnalyser
from helios.utils.helpers import current_quarter


class EarningsCallAnalyser(ManagedAgentAnalyser):
    """Earnings call transcript analysis via managed AI agent."""

    AGENT_SPEC_FILE = "earnings_calls_analysis.md"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.force_recompute = self.force_recompute or EXTRACTION_FORCE.earnings

    def _get_model(self) -> str:
        return GEMINI.earnings_call_analysis_model

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        years_back = EDGAR.years_back_earnings_calls
        starting_year = int(year) - years_back

        return os.path.join(
            self._ticker_output_dir(OutputDir.EARNINGS_CALLS),
            f"from_{starting_year}-Q{quarter}_to_{year}-Q{quarter}.md",
        )

    def _build_agent_spec(self) -> str:
        now = datetime.now()
        years_back = EDGAR.years_back_earnings_calls
        starting_date = f"{now.year - years_back}{now.strftime('-%m-%d')}"

        return self.spec_renderer.render(
            self.AGENT_SPEC_FILE, TICKER=self.ticker, FROM_DATE=starting_date, TO_DATE=now.strftime("%Y-%m-%d")
        )
