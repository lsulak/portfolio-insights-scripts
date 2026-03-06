"""Business Overview Synthesizer — structured executive primer on the business.

Consumes outputs from five prior synthesis/extraction stages:
    1. Narrative Validation    (Markdown — forensic audit report)
    2. Quantitative Baseline   (YAML — segment revenue/margin evolution)
    3. Sector Analysis         (Markdown — industry rivalry & peers)
    4. Earnings Calls          (Markdown — management commentary)
    5. Edgar Filings           (JSON — all 8-Ks, all 10-Qs, last 10-K only)

and feeds them as a single compiled dossier to a Gemini model with the
``business_overview.md`` agent spec.

The result is a Markdown executive primer covering origin, revenue engine,
cost structure, execution milestones, and moat analysis.
"""

import logging
import os

from helios.config import GEMINI, SYNTHESIS_AGENT_SPECS_DIR, OutputDir
from helios.extraction_engine.edgar.domain import EdgarFormType
from helios.utils.commons import DossierAnalyser, current_quarter

logger = logging.getLogger(__name__)


class BusinessOverviewSynthesizer(DossierAnalyser):
    """Produces a structured business primer for a ticker.

    Gathers narrative validation, quantitative baseline, sector analysis,
    earnings call synthesis, and selective Edgar filings (all 8-Ks, all 10-Qs,
    last 10-K only), then feeds them to a Gemini model with the business
    overview agent spec to produce a Markdown executive primer.
    """

    AGENT_SPEC_FILE = "business_overview.md"
    AGENT_SPECS_DIR = SYNTHESIS_AGENT_SPECS_DIR

    def _get_model(self) -> str:
        return GEMINI.business_overview_model

    def _get_temperature(self) -> float:
        return GEMINI.business_overview_temperature

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        filename = f"overview_{year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.BUSINESS_OVERVIEW, filename)

    def _compile_dossier(self) -> str:
        sections: list[str] = []
        self._add_dossier_section(
            sections, "NARRATIVE VALIDATION PAYLOAD", self._collect_files(OutputDir.NARRATIVE_VALIDATION, "*.md")
        )
        self._add_dossier_section(
            sections,
            "QUANTITATIVE BASELINE PAYLOAD (YAML)",
            self._collect_files(OutputDir.QUANTITATIVE_BASELINE, "*.yaml"),
        )
        self._add_dossier_section(
            sections, "SECTOR ANALYSIS PAYLOAD", self._collect_files(OutputDir.SECTOR_ANALYSIS, "*.md")
        )
        self._add_dossier_section(
            sections, "EARNINGS CALL SYNTHESIS", self._collect_files(OutputDir.EARNINGS_CALLS, "*.md")
        )
        self._add_dossier_section(
            sections,
            "EDGAR FILINGS (8-K all, 10-Q all, 10-K latest only)",
            self._collect_edgar_filings(
                form_types=[EdgarFormType.CURRENT_REPORT, EdgarFormType.QUARTERLY_REPORT, EdgarFormType.ANNUAL_REPORT],
                max_per_type={EdgarFormType.ANNUAL_REPORT: 1},
            ),
        )

        if not sections:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. " f"Run the extraction and synthesis engines first."
            )
        return "\n".join(sections)
