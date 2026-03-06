"""Business Overview Synthesizer — structured executive primer on the business.

Consumes outputs from five prior synthesis/extraction stages:
    1. Narrative Validation    (Markdown — forensic audit report)
    2. Quantitative Baseline   (YAML — segment revenue/margin evolution)
    3. Sector Analysis         (Markdown — industry rivalry & peers)
    4. Earnings Calls          (Markdown — management commentary)
    5. Edgar Filings           (JSON — last 10-K only)

and passes them as context to a Gemini Deep Research Agent with the
``business_overview.md`` agent spec.

The result is a Markdown executive primer covering origin, revenue engine,
cost structure, execution milestones, and moat analysis.
"""

import logging
import os
from pathlib import Path

from helios.config import GEMINI, SYNTHESIS_AGENT_SPECS_DIR, OutputDir
from helios.extraction_engine.edgar.domain import EdgarFormType
from helios.utils.deep_research_analyser import DeepResearchAnalyser
from helios.utils.commons import format_dossier_section, read_dir_files

logger = logging.getLogger(__name__)


class BusinessOverviewSynthesizer(DeepResearchAnalyser):
    """Produces a structured business primer for a ticker via Deep Research.

    Gathers narrative validation, quantitative baseline, sector analysis, earnings call synthesis,
    and selective Edgar filings - last 10-K only, then feeds them as context to a Gemini
    Deep Research Agent with the business overview agent spec.
    """

    AGENT_SPEC_FILENAME = "business_overview.md"
    AGENT_SPECS_DIR = SYNTHESIS_AGENT_SPECS_DIR

    def _get_model(self) -> str:
        return GEMINI.business_overview_model

    def _build_output_path(self) -> str:
        year, quarter = self._current_quarter()
        filename = f"overview_{year}-Q{quarter}.md"
        return os.path.join(self._ticker_output_dir(OutputDir.BUSINESS_OVERVIEW), filename)

    def _build_agent_spec(self) -> str:
        return self._render_agent_spec(self.AGENT_SPEC_FILENAME, TICKER=self.ticker)

    def _build_context(self) -> str:
        """Load upstream pipeline outputs as context for Deep Research."""
        ticker_dir = os.path.join(self.output_base_dir, self.ticker)

        # Latest 10-K only
        edgar_base = os.path.join(ticker_dir, OutputDir.EDGAR_SUMMARIZED)
        form_dir = os.path.join(edgar_base, EdgarFormType.ANNUAL_REPORT)
        edgar_docs: list[tuple[str, str]] = []
        if os.path.isdir(form_dir):
            files = sorted(Path(form_dir).glob("*.json"), reverse=True)[:1]
            for fp in files:
                content = fp.read_text(encoding="utf-8")
                if content:
                    edgar_docs.append((f"EDGAR {EdgarFormType.ANNUAL_REPORT}/{fp.name} (latest only)", content))

        sections = [
            format_dossier_section("EDGAR FILINGS (10-K latest only)", edgar_docs),
            format_dossier_section(
                "QUANTITATIVE BASELINE PAYLOAD (YAML)",
                read_dir_files(os.path.join(ticker_dir, OutputDir.QUANTITATIVE_BASELINE), "*.yaml"),
            ),
            format_dossier_section(
                "NARRATIVE VALIDATION PAYLOAD",
                read_dir_files(os.path.join(ticker_dir, OutputDir.NARRATIVE_VALIDATION), "*.md"),
            ),
            format_dossier_section(
                "SECTOR ANALYSIS PAYLOAD",
                read_dir_files(os.path.join(ticker_dir, OutputDir.SECTOR_ANALYSIS), "*.md"),
            ),
            format_dossier_section(
                "EARNINGS CALL SYNTHESIS",
                read_dir_files(os.path.join(ticker_dir, OutputDir.EARNINGS_CALLS), "*.md"),
            ),
        ]

        dossier = "\n\n".join(s for s in sections if s)
        if not dossier:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. Run the extraction and synthesis engines first."
            )

        logger.info(f"[BusinessOverviewSynthesizer] Loaded context ({len(dossier):,} chars).")
        return dossier
