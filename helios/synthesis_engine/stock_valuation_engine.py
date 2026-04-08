"""Stock Valuation Engine — deterministic DCF and relative valuation.

Consumes outputs from five prior synthesis/extraction stages:
    1. Quantitative Baseline  (YAML — financial triad ledger)
    2. Narrative Validation   (Markdown — forensic audit report)
    3. Sector Analysis        (Markdown — industry rivalry & peers)
    4. Market Analysis        (Markdown — macro cycle & pricing)
    5. Earnings Calls         (Markdown — forward guidance)

and feeds them as a single compiled document to a stateless model call with the
``stock_valuation.md`` agent spec.

The result is a Markdown valuation report containing intrinsic value,
relative valuation, reverse DCF, and margin of safety.
"""

import logging
import os

from helios.config import GEMINI, OutputDir
from helios.pipeline.stateless_model import StatelessModelAnalyser
from helios.utils.helpers import current_quarter, format_section

logger = logging.getLogger(__name__)


class StockValuationEngine(StatelessModelAnalyser):
    """Executes a Damodaran-style DCF valuation for a ticker.

    Gathers the quantitative baseline ledger, narrative validation report,
    sector analysis, market analysis, and earnings call synthesis, then feeds
    them to a Gemini model with the stock valuation agent spec to produce a
    Markdown valuation report.
    """

    AGENT_SPEC_FILE = "stock_valuation.md"

    def _get_model(self) -> str:
        return GEMINI.stock_valuation_model

    def _get_temperature(self) -> float:
        return GEMINI.stock_valuation_temperature

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        filename = f"valuation_{year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.STOCK_VALUATION, filename)

    def _compile_sources(self) -> str:
        sections = [
            format_section(
                "QBC: QUANTITATIVE BASELINE PAYLOAD (YAML)",
                self._collect_files(OutputDir.QUANTITATIVE_BASELINE, "*.yaml"),
            ),
            format_section(
                "NV: NARRATIVE VALIDATION PAYLOAD", self._collect_files(OutputDir.NARRATIVE_VALIDATION, "*.md")
            ),
            format_section("SE: SECTOR ANALYSIS PAYLOAD", self._collect_files(OutputDir.SECTOR_ANALYSIS, "*.md")),
            format_section("ME: MARKET ANALYSIS PAYLOAD", self._collect_files(OutputDir.MARKET_ANALYSIS, "*.md")),
            format_section("ETE: EARNINGS CALL SYNTHESIS", self._collect_files(OutputDir.EARNINGS_CALLS, "*.md")),
        ]
        return self._join_sections(sections)
