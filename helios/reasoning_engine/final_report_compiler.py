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

import json
import logging
import os

from helios.config import GEMINI, REASONING_AGENT_SPECS_DIR, OutputDir
from helios.synthesis_engine.dcf_calculator import DCFResults
from helios.utils.commons import current_quarter, format_dossier_section
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
        year, quarter = current_quarter()
        filename = f"report_{year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.FINAL_REPORT, filename)
    
    def _read_dcf_results(self) -> DCFResults:
        year, quarter = current_quarter()
        filename = f"dcf_results_{year}-Q{quarter}.json"
        full_path = os.path.join(self._ticker_dir(), OutputDir.STOCK_VALUATION, filename)

        if os.path.exists(full_path):
            with open(full_path, "r") as f:
                dcf_results_data = json.load(f)
                logger.info("Successfully read DCF results from '%s': %s", full_path, dcf_results_data)
                return DCFResults(**dcf_results_data)
        else:
            raise Exception(
                f"DCF results file not found at '{full_path}'. "
                f"Ensure the Stock Valuation Engine has run and produced the expected output."
            )
        
    def _format_dcf_results_section(self, dcf_results: DCFResults) -> str:
        return f"""
        {"=" * 60} PYTHON_DCF_RESULTS
        - **Intrinsic Value Per Share:** ${dcf_results.intrinsic_value_per_share}
        - **Current Stock Price:** ${dcf_results.current_stock_price}
        - **Margin of Safety:** {dcf_results.margin_of_safety_percent}%
        - **Calculated Firm Value:** ${dcf_results.calculated_firm_value}
        - **Calculated Equity Value:** ${dcf_results.calculated_equity_value}
        - **Growth Dependency Ratio (Terminal Value %):** {dcf_results.growth_dependency_ratio * 100}%
        - **Integrity Haircut Applied:** {dcf_results.integrity_haircut_applied}
        """
    
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

        dcf_results = self._read_dcf_results()
        python_dcf_payload = self._format_dcf_results_section(dcf_results)
        sections.append(python_dcf_payload)

        dossier = "\n\n".join(s for s in sections if s)
        logger.debug("Compiled dossier content:\n%s", dossier)

        if not dossier:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. "
                f"Run the extraction, synthesis, and reasoning engines first."
            )
        return dossier
