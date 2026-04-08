"""External Reality Checker — managed agent for company analysis
beyond official company filings — suppliers, customers, controversies, etc.

Uses Business Overview and Narrative Validation reports as context so the
agent can cross-reference its internet findings against the company's own
filings.
"""

import logging
import os

from helios.config import GEMINI, OutputDir
from helios.pipeline.managed_agent import ManagedAgentAnalyser
from helios.utils.helpers import current_quarter, format_section

logger = logging.getLogger(__name__)


class ExternalRealityChecker(ManagedAgentAnalyser):
    """External Reality checker via managed AI agent."""

    AGENT_SPEC_FILE = "external_reality_check.md"

    def _get_model(self) -> str:
        return GEMINI.external_reality_check_model

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        return os.path.join(
            self._ticker_output_dir(OutputDir.EXTERNAL_REALITY_CHECK), f"report_during_{year}-Q{quarter}.md"
        )

    def _build_context(self) -> str:
        """Load Business Overview and Narrative Validation reports as context."""
        sections = [
            format_section("BE: BUSINESS OVERVIEW", self._collect_files(OutputDir.BUSINESS_OVERVIEW, "*.md")),
            format_section("NV: NARRATIVE VALIDATION", self._collect_files(OutputDir.NARRATIVE_VALIDATION, "*.md")),
        ]

        context = "\n\n".join(s for s in sections if s)
        if not context:
            raise Exception("No Business Overview/Narrative Validation context found. Cannot run External Reality Checker.")

        logger.info(f"Loaded Business Overview + Narrative Validation context ({len(context):,} chars).")
        return context
