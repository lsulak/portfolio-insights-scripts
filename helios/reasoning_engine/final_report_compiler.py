"""Final Report Compiler — the capstone HELIOS investment report.

Consumes outputs from seven prior pipeline stages:
    1. Quantitative Baseline    (YAML — segment revenue/margin evolution)
    2. Narrative Validation     (Markdown — forensic audit report)
    3. Sector Analysis          (Markdown — industry rivalry & peers)
    4. Market Analysis          (Markdown — macro cycle & liquidity)
    5. Stock Valuation          (Markdown — DCF, reverse DCF, relative valuation)
    6. Business Overview        (Markdown — executive primer on the business)
    7. External Reality Check   (Markdown — scuttlebutt & short-seller claims)

and feeds them as a single compiled document to a stateless model call with the
``final_report.md`` agent spec.

The result is a comprehensive Markdown investment report covering company
overview, financial strength, business quality, growth, cycles, a pre-mortem,
and the HELIOS Committee Verdict with an allocation decision.
"""

import datetime
import json
import logging
import os
from dataclasses import asdict

from helios.config import GEMINI, OutputDir
from helios.synthesis_engine.dcf_calculator import DCFResults
from helios.pipeline.stateless_model import StatelessModelAnalyser
from helios.utils.helpers import current_quarter, format_section

logger = logging.getLogger(__name__)


class FinalReportCompiler(StatelessModelAnalyser):
    """Produces the capstone HELIOS investment report for a ticker.

    Gathers quantitative baseline, narrative validation, sector analysis,
    market analysis, stock valuation, business overview, and external reality
    check, then feeds them to a Gemini model with the final report agent spec
    to produce a comprehensive Markdown investment report.
    """

    AGENT_SPEC_FILE = "final_report.md"

    def _get_model(self) -> str:
        return GEMINI.final_report_model

    def _get_temperature(self) -> float:
        return GEMINI.final_report_temperature

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        filename = f"report_{year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.FINAL_REPORT, filename)

    def _read_dcf_results(self) -> DCFResults:
        """Load the deterministic DCF results produced by ``DCFCalculator``."""
        year, quarter = current_quarter()
        filename = f"dcf_results_{year}-Q{quarter}.json"
        full_path = os.path.join(self._ticker_dir(), OutputDir.STOCK_VALUATION, filename)

        if not os.path.exists(full_path):
            raise FileNotFoundError(
                f"DCF results file not found at '{full_path}'. "
                "Ensure the DCF Calculator has run before the Final Report."
            )

        with open(full_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("Loaded DCF results from '%s': %s", full_path, data)
        return DCFResults(**data)

    def _build_agent_spec(self) -> str:
        now = datetime.datetime.now()
        try:
            five_years_from_now = now.replace(year=now.year + 5).strftime("%Y-%m-%d")
        except ValueError:
            # Feb 29 → Feb 28 in a non-leap year
            five_years_from_now = now.replace(year=now.year + 5, day=28).strftime("%Y-%m-%d")
        return self.spec_renderer.render(self.AGENT_SPEC_FILE, TICKER=self.ticker, FUTURE_DATE_5Y=five_years_from_now)

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
            format_section("VE: VALUATION ENGINE PAYLOAD", self._collect_files(OutputDir.STOCK_VALUATION, "*.md")),
            format_section("BE: BUSINESS OVERVIEW PAYLOAD", self._collect_files(OutputDir.BUSINESS_OVERVIEW, "*.md")),
            format_section(
                "ERC: EXTERNAL REALITY CHECK PAYLOAD",
                self._collect_files(OutputDir.EXTERNAL_REALITY_CHECK, "*.md"),
            ),
        ]

        compiled = self._join_sections(sections)

        # Append deterministic DCF results (auto-formats from dataclass fields)
        dcf_results = self._read_dcf_results()
        dcf_text = "\n".join(f"  {key}: {value}" for key, value in asdict(dcf_results).items())
        dcf_section = format_section("PYTHON_DCF_RESULTS", [("dcf_calculation_output", dcf_text)])
        compiled = f"{compiled}\n\n{dcf_section}"

        logger.debug("Compiled source content:\n%s", compiled)
        return compiled
