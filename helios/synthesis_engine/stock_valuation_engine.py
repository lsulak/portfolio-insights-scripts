"""Stock Valuation Engine — deterministic DCF and relative valuation.

Consumes outputs from four prior synthesis/extraction stages:
    1. Quantitative Baseline  (YAML — financial triad ledger)
    2. Narrative Validation   (Markdown — forensic audit report)
    3. Sector Analysis        (Markdown — industry rivalry & peers)
    4. Market Analysis        (Markdown — macro cycle & pricing)

and feeds them as a single compiled dossier to a Gemini model with the
``stock_valuation.md`` agent spec.

The result is a Markdown valuation report containing intrinsic value,
relative valuation, reverse DCF, and margin of safety.
"""

import logging
import os

from helios.config import GEMINI, SYNTHESIS_AGENT_SPECS_DIR, OutputDir
from helios.utils.commons import format_dossier_section
from helios.utils.dossier_analyser import DossierAnalyser

logger = logging.getLogger(__name__)


class StockValuationEngine(DossierAnalyser):
    """Executes a Damodaran-style DCF valuation for a ticker.

    Gathers the quantitative baseline ledger, narrative validation report,
    sector analysis, and market analysis, then feeds them to a Gemini model
    with the stock valuation agent spec to produce a Markdown valuation report.
    """

    AGENT_SPEC_FILE = "stock_valuation.md"
    AGENT_SPECS_DIR = SYNTHESIS_AGENT_SPECS_DIR

    def _get_model(self) -> str:
        return GEMINI.stock_valuation_model

    def _get_temperature(self) -> float:
        return GEMINI.stock_valuation_temperature

    def _build_output_path(self) -> str:
        year, quarter = self._current_quarter()
        filename = f"valuation_{year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.STOCK_VALUATION, filename)

    def _compile_dossier(self) -> str:
        sections = [
            format_dossier_section(
                "QUANTITATIVE BASELINE PAYLOAD (YAML)",
                self._collect_files(OutputDir.QUANTITATIVE_BASELINE, "*.yaml"),
            ),
            format_dossier_section(
                "NARRATIVE VALIDATION PAYLOAD", self._collect_files(OutputDir.NARRATIVE_VALIDATION, "*.md")
            ),
            format_dossier_section("SECTOR ANALYSIS PAYLOAD", self._collect_files(OutputDir.SECTOR_ANALYSIS, "*.md")),
            format_dossier_section("MARKET ANALYSIS PAYLOAD", self._collect_files(OutputDir.MARKET_ANALYSIS, "*.md")),
        ]
        dossier = "\n\n".join(s for s in sections if s)
        if not dossier:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. Run the synthesis and extraction engines first."
            )
        return dossier
