"""Earnings Call analyser — Deep Research Agent for earnings transcripts."""

import os
from datetime import datetime

from helios.config import GEMINI, EDGAR
from helios.utils.commons import DeepResearchAnalyser


class EarningsCallAnalyser(DeepResearchAnalyser):
    """Earnings call transcript analysis via Gemini Deep Research Agent."""

    OUT_DIR_NAME = "earnings_calls_synthesis"
    AGENT_SPEC_FILENAME = "earnings_calls.md"

    def _get_model(self) -> str:
        return GEMINI.earnings_call_model

    def _build_output_path(self) -> str:
        year, quarter = self._current_quarter()
        years_back = EDGAR.years_back_earnings_calls
        starting_year = int(year) - years_back

        return os.path.join(
            self._ticker_output_dir(self.OUT_DIR_NAME),
            f"from_{starting_year}-Q{quarter}_to_{year}-Q{quarter}.md",
        )

    def _build_agent_spec(self) -> str:
        now = datetime.now()
        years_back = EDGAR.years_back_earnings_calls
        starting_date = f"{now.year - years_back}{now.strftime('-%m-%d')}"

        return self._render_agent_spec(
            self.AGENT_SPEC_FILENAME, TICKER=self.ticker, FROM_DATE=starting_date, TO_DATE=now.strftime("%Y-%m-%d")
        )
