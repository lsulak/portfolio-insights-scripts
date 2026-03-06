"""Final Report Compiler — the capstone HELIOS investment report.

Consumes outputs from seven prior pipeline stages:
    1. Quantitative Baseline    (YAML — segment revenue/margin evolution)
    2. Narrative Validation     (Markdown — forensic audit report)
    3. Sector Analysis          (Markdown — industry rivalry & peers)
    4. Market Analysis          (Markdown — macro cycle & liquidity)
    5. Stock Valuation          (Markdown — DCF, reverse DCF, relative valuation)
    6. Business Overview        (Markdown — executive primer on the business)
    7. External Reality Check   (Markdown — scuttlebutt & short-seller claims)

and feeds them as a single compiled dossier to a Gemini model with the
``final_report.md`` agent spec.

The result is a comprehensive Markdown investment report covering company
overview, financial strength, business quality, growth, cycles, a pre-mortem,
and the HELIOS Committee Verdict with an allocation decision.
"""

import logging
import os

from helios.config import GEMINI, REASONING_AGENT_SPECS_DIR, OutputDir
from helios.utils.commons import format_dossier_section
from helios.utils.dossier_analyser import DossierAnalyser

logger = logging.getLogger(__name__)


class FinalReportCompiler(DossierAnalyser):
    """Produces the capstone HELIOS investment report for a ticker.

    Gathers quantitative baseline, narrative validation, sector analysis,
    market analysis, stock valuation, business overview, and external reality
    check, then feeds them to a Gemini model with the final report agent spec
    to produce a comprehensive Markdown investment report.
    """

    AGENT_SPEC_FILE = "final_report.md"
    AGENT_SPECS_DIR = REASONING_AGENT_SPECS_DIR

    def _get_model(self) -> str:
        return GEMINI.final_report_model

    def _get_temperature(self) -> float:
        return GEMINI.final_report_temperature

    def _build_output_path(self) -> str:
        year, quarter = self._current_quarter()
        filename = f"report_{year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.FINAL_REPORT, filename)

    def _compile_dossier(self) -> str:
        sections = [
            format_dossier_section(
                "QBC: QUANTITATIVE BASELINE PAYLOAD (YAML)",
                self._collect_files(OutputDir.QUANTITATIVE_BASELINE, "*.yaml"),
            ),
            format_dossier_section(
                "NV: NARRATIVE VALIDATION PAYLOAD", self._collect_files(OutputDir.NARRATIVE_VALIDATION, "*.md")
            ),
            format_dossier_section(
                "SE: SECTOR ANALYSIS PAYLOAD", self._collect_files(OutputDir.SECTOR_ANALYSIS, "*.md")
            ),
            format_dossier_section(
                "ME: MARKET ANALYSIS PAYLOAD", self._collect_files(OutputDir.MARKET_ANALYSIS, "*.md")
            ),
            format_dossier_section(
                "VE: VALUATION ENGINE PAYLOAD", self._collect_files(OutputDir.STOCK_VALUATION, "*.md")
            ),
            format_dossier_section(
                "BE: BUSINESS OVERVIEW PAYLOAD", self._collect_files(OutputDir.BUSINESS_OVERVIEW, "*.md")
            ),
            format_dossier_section(
                "ERC: EXTERNAL REALITY CHECK PAYLOAD",
                self._collect_files(OutputDir.EXTERNAL_REALITY_CHECK, "*.md"),
            ),
        ]
        dossier = "\n\n".join(s for s in sections if s)
        if not dossier:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. "
                f"Run the extraction, synthesis, and reasoning engines first."
            )
        return dossier
