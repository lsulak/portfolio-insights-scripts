"""Quantitative Baseline Compiler — deterministic financial data aggregator.

Consumes summarized Edgar filings (JSON, grouped by form type, latest first)
and feeds them as a single compiled document to a stateless model call with the
``quantitative_baseline.md`` agent spec.

The result is a YAML ledger containing the complete financial triad.
"""

import logging
import os

from helios.config import GEMINI, OutputDir
from helios.extraction_engine.edgar.reader import collect_edgar_filings
from helios.pipeline.stateless_model import StatelessModelAnalyser
from helios.utils.helpers import current_quarter, format_section

logger = logging.getLogger(__name__)


class QuantitativeBaselineCompiler(StatelessModelAnalyser):
    """Compiles a deterministic YAML financial ledger for a ticker.

    Gathers summarized Edgar filings and feeds them to a Gemini model with
    the quantitative baseline compiler agent spec to produce a chronologically
    ordered YAML ledger.
    """

    AGENT_SPEC_FILE = "quantitative_baseline.md"

    def _get_model(self) -> str:
        return GEMINI.quantitative_baseline_model

    def _get_temperature(self) -> float:
        return GEMINI.quantitative_baseline_temperature

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        filename = f"ledger_{year}-Q{quarter}.yaml"
        return os.path.join(self._ticker_dir(), OutputDir.QUANTITATIVE_BASELINE, filename)

    def _compile_sources(self) -> str:
        sections = [
            format_section("EE: EDGAR FILINGS (latest first)", collect_edgar_filings(self._ticker_dir())),
        ]
        return self._join_sections(sections)
