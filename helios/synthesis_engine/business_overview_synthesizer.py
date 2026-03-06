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
from helios.utils.commons import DeepResearchAnalyser, read_dir_files

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
        sections: list[str] = []

        # Latest 10-K only
        edgar_base = os.path.join(ticker_dir, OutputDir.EDGAR_SUMMARIZED)
        form_dir = os.path.join(edgar_base, EdgarFormType.ANNUAL_REPORT)
        if os.path.isdir(form_dir):
            files = sorted(Path(form_dir).glob("*.json"), reverse=True)[:1]
            edgar_docs = []
            for fp in files:
                content = fp.read_text(encoding="utf-8")
                if content:
                    edgar_docs.append((f"EDGAR {EdgarFormType.ANNUAL_REPORT}/{fp.name} (latest only)", content))
            if edgar_docs:
                self._add_section(sections, "EDGAR FILINGS (10-K latest only)", edgar_docs)

        # Quantitative Baseline
        qbc_docs = read_dir_files(os.path.join(ticker_dir, OutputDir.QUANTITATIVE_BASELINE), "*.yaml")
        if qbc_docs:
            self._add_section(sections, "QUANTITATIVE BASELINE PAYLOAD (YAML)", qbc_docs)

        # Narrative Validation
        nv_docs = read_dir_files(os.path.join(ticker_dir, OutputDir.NARRATIVE_VALIDATION), "*.md")
        if nv_docs:
            self._add_section(sections, "NARRATIVE VALIDATION PAYLOAD", nv_docs)

        # Sector Analysis
        se_docs = read_dir_files(os.path.join(ticker_dir, OutputDir.SECTOR_ANALYSIS), "*.md")
        if se_docs:
            self._add_section(sections, "SECTOR ANALYSIS PAYLOAD", se_docs)

        # Earnings Calls
        ec_docs = read_dir_files(os.path.join(ticker_dir, OutputDir.EARNINGS_CALLS), "*.md")
        if ec_docs:
            self._add_section(sections, "EARNINGS CALL SYNTHESIS", ec_docs)

        if not sections:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. Run the extraction and synthesis engines first."
            )

        logger.info(f"[BusinessOverviewSynthesizer] Loaded context ({sum(len(s) for s in sections):,} chars).")
        return "\n".join(sections)

    @staticmethod
    def _add_section(sections: list[str], heading: str, docs: list[tuple[str, str]]) -> None:
        """Append a labeled document group to sections."""
        separator = "=" * 60
        prefix = "" if not sections else "\n"
        sections.append(prefix + separator)
        sections.append(heading)
        sections.append(separator)
        for label, content in docs:
            sections.append(f"\n--- {label} ---\n")
            sections.append(content)
