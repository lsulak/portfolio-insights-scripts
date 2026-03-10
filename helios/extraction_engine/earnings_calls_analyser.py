"""Earnings Call analyser — Deep Research Agent for earnings transcripts."""

import os
from datetime import datetime

from helios.config import GEMINI, EDGAR, OutputDir
from helios.utils.commons import current_quarter
from helios.utils.deep_research_analyser import DeepResearchAnalyser


class EarningsCallAnalyser(DeepResearchAnalyser):
    """Earnings call transcript analysis via Gemini Deep Research Agent."""

    AGENT_SPEC_FILENAME = "earnings_calls_analysis.md"

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

        return self._render_agent_spec(
            self.AGENT_SPEC_FILENAME, TICKER=self.ticker, FROM_DATE=starting_date, TO_DATE=now.strftime("%Y-%m-%d")
        )
