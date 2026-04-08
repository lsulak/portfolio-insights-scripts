"""Business Overview Synthesizer — structured executive primer on the business.

Consumes outputs from six prior synthesis/extraction stages:
    1. Quantitative Baseline   (YAML — segment revenue/margin evolution)
    2. Sector Analysis         (Markdown — industry rivalry & peers)
    3. Earnings Calls          (Markdown — management commentary)
    4. Edgar Filings           (JSON — last 10-K only)
    5. Edgar 8-K Filings       (JSON — material events)

and passes them as context to a managed AI agent with the
``business_overview.md`` agent spec.

The result is a Markdown executive primer covering origin, revenue engine,
cost structure, execution milestones, and moat analysis.
"""

import logging
import os
from pathlib import Path

from helios.config import GEMINI, OutputDir
from helios.extraction_engine.edgar.domain import EdgarFormType
from helios.pipeline.managed_agent import ManagedAgentAnalyser
from helios.utils.helpers import current_quarter, format_section

logger = logging.getLogger(__name__)


class BusinessOverviewSynthesizer(ManagedAgentAnalyser):
    """Produces a structured business primer for a ticker via a managed AI agent.

    Gathers quantitative baseline, sector analysis, earnings call synthesis,
    selective Edgar filings (last 10-K + 8-K material events), then feeds them
    as context to the agent with the business overview agent spec.
    """

    AGENT_SPEC_FILE = "business_overview.md"

    def _get_model(self) -> str:
        return GEMINI.business_overview_model

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        filename = f"overview_{year}-Q{quarter}.md"
        return os.path.join(self._ticker_output_dir(OutputDir.BUSINESS_OVERVIEW), filename)

    def _build_context(self) -> str:
        """Load upstream pipeline outputs as context for Deep Research."""
        # Latest 10-K only
        edgar_base = os.path.join(self._ticker_dir(), OutputDir.EDGAR_SUMMARIZED)
        form_dir = os.path.join(edgar_base, EdgarFormType.ANNUAL_REPORT)
        edgar_10k_docs: list[tuple[str, str]] = []
        if os.path.isdir(form_dir):
            files = sorted(Path(form_dir).glob("*.json"), reverse=True)[:1]
            for fp in files:
                content = fp.read_text(encoding="utf-8")
                if content:
                    edgar_10k_docs.append((f"EDGAR {EdgarFormType.ANNUAL_REPORT}/{fp.name} (latest only)", content))

        # 8-K material events
        form_dir_8k = os.path.join(edgar_base, EdgarFormType.CURRENT_REPORT)
        edgar_8k_docs: list[tuple[str, str]] = []
        if os.path.isdir(form_dir_8k):
            files = sorted(Path(form_dir_8k).glob("*.json"), reverse=True)
            for fp in files:
                content = fp.read_text(encoding="utf-8")
                if content:
                    edgar_8k_docs.append((f"EDGAR {EdgarFormType.CURRENT_REPORT}/{fp.name}", content))

        sections = [
            format_section("EE: EDGAR FILINGS (10-K latest only)", edgar_10k_docs),
            format_section("EE: EDGAR MATERIAL EVENTS (8-K)", edgar_8k_docs),
            format_section(
                "QBC: QUANTITATIVE BASELINE PAYLOAD (YAML)",
                self._collect_files(OutputDir.QUANTITATIVE_BASELINE, "*.yaml"),
            ),
            format_section(
                "SE: SECTOR ANALYSIS PAYLOAD",
                self._collect_files(OutputDir.SECTOR_ANALYSIS, "*.md"),
            ),
            format_section(
                "ETE: EARNINGS CALL SYNTHESIS",
                self._collect_files(OutputDir.EARNINGS_CALLS, "*.md"),
            ),
        ]

        context = self._join_sections(sections)
        logger.info(f"Loaded context ({len(context):,} chars) for BusinessOverviewSynthesizer.")
        return context
