"""Quantitative Baseline Compiler — deterministic financial data aggregator.

Consumes outputs from two extraction engine analysers:
    1. Earnings Calls     (Markdown — forward guidance numbers only)
    2. Edgar Filings      (JSON, grouped by form type, latest first)

and feeds them as a single compiled dossier to a Gemini model with the
``quantitative_baseline_compiler.md`` agent spec.

The result is a YAML ledger containing the complete financial triad.
"""

import logging
import os

from helios.config import GEMINI, SYNTHESIS_AGENT_SPECS_DIR, OutputDir
from helios.utils.commons import format_dossier_section
from helios.utils.dossier_analyser import DossierAnalyser

logger = logging.getLogger(__name__)


class QuantitativeBaselineCompiler(DossierAnalyser):
    """Compiles a deterministic YAML financial ledger for a ticker.

    Gathers earnings call synthesis and summarized Edgar filings, then feeds
    them to a Gemini model with the quantitative baseline compiler agent spec
    to produce a chronologically ordered YAML ledger.
    """

    AGENT_SPEC_FILE = "quantitative_baseline.md"
    AGENT_SPECS_DIR = SYNTHESIS_AGENT_SPECS_DIR

    def _get_model(self) -> str:
        return GEMINI.quantitative_baseline_model

    def _get_temperature(self) -> float:
        return GEMINI.quantitative_baseline_temperature

    def _build_output_path(self) -> str:
        year, quarter = self._current_quarter()
        filename = f"ledger_{year}-Q{quarter}.yaml"
        return os.path.join(self._ticker_dir(), OutputDir.QUANTITATIVE_BASELINE, filename)

    def _compile_dossier(self) -> str:
        sections = [
            format_dossier_section("EARNINGS CALL SYNTHESIS", self._collect_files(OutputDir.EARNINGS_CALLS, "*.md")),
            format_dossier_section("EDGAR FILINGS (latest first)", self._collect_edgar_filings()),
        ]
        dossier = "\n\n".join(s for s in sections if s)
        if not dossier:
            raise FileNotFoundError(f"No source documents found for {self.ticker}. Run the extraction engine first.")
        return dossier
