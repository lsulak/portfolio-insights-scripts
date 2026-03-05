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
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Generator

from google import genai

from helios.config import GEMINI, REASONING_AGENT_SPECS_DIR, OutputDir
from helios.utils.commons import (
    AIHostedFile,
    GeminiExtractorAgent,
    GeminiFileManager,
    ResponseTypes,
    generate_agent_spec,
    load_agent_spec,
)

logger = logging.getLogger(__name__)


class FinalReportCompiler:
    """Produces the capstone HELIOS investment report for a ticker.

    Gathers quantitative baseline, narrative validation, sector analysis,
    market analysis, stock valuation, business overview, and external reality
    check, then feeds them to a Gemini model with the final report agent spec
    to produce a comprehensive Markdown investment report.
    """

    AGENT_SPEC_FILE = "final_report.md"

    def __init__(
        self,
        client: genai.Client,
        output_base_dir: str,
        ticker: str,
        force_resummarize: bool = False,
    ) -> None:
        self.client = client
        self.output_base_dir = output_base_dir
        self.ticker = ticker
        self.force_resummarize = force_resummarize

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _ticker_dir(self) -> str:
        return os.path.join(self.output_base_dir, self.ticker)

    def _build_output_path(self) -> str:
        now = datetime.now()
        quarter = ((now.month - 1) // 3) + 1
        filename = f"report_{now.year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.FINAL_REPORT, filename)

    # ------------------------------------------------------------------
    # Document collection
    # ------------------------------------------------------------------

    def _collect_quantitative_baseline(self) -> list[tuple[str, str]]:
        """Read the quantitative baseline YAML ledger(s)."""
        baseline_dir = os.path.join(self._ticker_dir(), OutputDir.QUANTITATIVE_BASELINE)
        return list(self._read_dir_files(baseline_dir, "*.yaml"))

    def _collect_narrative_validation(self) -> list[tuple[str, str]]:
        """Read the narrative validation report(s) (Markdown)."""
        narrative_dir = os.path.join(self._ticker_dir(), OutputDir.NARRATIVE_VALIDATION)
        return list(self._read_dir_files(narrative_dir, "*.md"))

    def _collect_sector_analysis(self) -> list[tuple[str, str]]:
        """Read all sector analysis reports (Markdown)."""
        sector_dir = os.path.join(self._ticker_dir(), OutputDir.SECTOR_ANALYSIS)
        return list(self._read_dir_files(sector_dir, "*.md"))

    def _collect_market_analysis(self) -> list[tuple[str, str]]:
        """Read all market analysis reports (Markdown)."""
        market_dir = os.path.join(self._ticker_dir(), OutputDir.MARKET_ANALYSIS)
        return list(self._read_dir_files(market_dir, "*.md"))

    def _collect_stock_valuation(self) -> list[tuple[str, str]]:
        """Read the stock valuation report(s) (Markdown)."""
        valuation_dir = os.path.join(self._ticker_dir(), OutputDir.STOCK_VALUATION)
        return list(self._read_dir_files(valuation_dir, "*.md"))

    def _collect_business_overview(self) -> list[tuple[str, str]]:
        """Read the business overview report(s) (Markdown)."""
        overview_dir = os.path.join(self._ticker_dir(), OutputDir.BUSINESS_OVERVIEW)
        return list(self._read_dir_files(overview_dir, "*.md"))

    def _collect_external_reality_check(self) -> list[tuple[str, str]]:
        """Read the external reality check report(s) (Markdown)."""
        erc_dir = os.path.join(self._ticker_dir(), OutputDir.EXTERNAL_REALITY_CHECK)
        return list(self._read_dir_files(erc_dir, "*.md"))

    @staticmethod
    def _read_dir_files(directory: str, pattern: str) -> Generator[tuple[str, str]]:
        """Read all files matching *pattern* in *directory*, sorted descending."""
        if not os.path.isdir(directory):
            raise Exception(f"Directory not found: {directory}")

        files = sorted(Path(directory).glob(pattern), reverse=True)

        for fp in files:
            if fp.stat().st_size == 0:
                raise Exception(f"Empty content in: {fp}.")

            yield (fp.name, fp.read_text(encoding="utf-8"))

    # ------------------------------------------------------------------
    # Dossier compilation
    # ------------------------------------------------------------------

    def _compile_dossier(self) -> str:
        """Concatenate all source documents into a structured text payload.

        Documents are ordered to match the agent spec's input data contract:
        QBC → NV → SE → ME → VE → BE → ERC.

        Raises:
            FileNotFoundError: If no source documents are found at all.
        """
        sections: list[str] = []

        # 1. Quantitative Baseline (YAML ledger)
        baseline_docs = self._collect_quantitative_baseline()
        if baseline_docs:
            sections.append("=" * 60)
            sections.append("QBC: QUANTITATIVE BASELINE PAYLOAD (YAML)")
            sections.append("=" * 60)
            for label, content in baseline_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 2. Narrative Validation (forensic audit)
        narrative_docs = self._collect_narrative_validation()
        if narrative_docs:
            sections.append("\n" + "=" * 60)
            sections.append("NV: NARRATIVE VALIDATION PAYLOAD")
            sections.append("=" * 60)
            for label, content in narrative_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 3. Sector Analysis
        sector_docs = self._collect_sector_analysis()
        if sector_docs:
            sections.append("\n" + "=" * 60)
            sections.append("SE: SECTOR ANALYSIS PAYLOAD")
            sections.append("=" * 60)
            for label, content in sector_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 4. Market Analysis
        market_docs = self._collect_market_analysis()
        if market_docs:
            sections.append("\n" + "=" * 60)
            sections.append("ME: MARKET ANALYSIS PAYLOAD")
            sections.append("=" * 60)
            for label, content in market_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 5. Stock Valuation
        valuation_docs = self._collect_stock_valuation()
        if valuation_docs:
            sections.append("\n" + "=" * 60)
            sections.append("VE: VALUATION ENGINE PAYLOAD")
            sections.append("=" * 60)
            for label, content in valuation_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 6. Business Overview
        overview_docs = self._collect_business_overview()
        if overview_docs:
            sections.append("\n" + "=" * 60)
            sections.append("BE: BUSINESS OVERVIEW PAYLOAD")
            sections.append("=" * 60)
            for label, content in overview_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 7. External Reality Check
        erc_docs = self._collect_external_reality_check()
        if erc_docs:
            sections.append("\n" + "=" * 60)
            sections.append("ERC: EXTERNAL REALITY CHECK PAYLOAD")
            sections.append("=" * 60)
            for label, content in erc_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        if not sections:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. "
                f"Run the extraction, synthesis, and reasoning engines first."
            )

        return "\n".join(sections)

    # ------------------------------------------------------------------
    # Agent spec
    # ------------------------------------------------------------------

    def _build_agent_spec(self) -> str:
        """Load and render the final report agent spec."""
        template = load_agent_spec(REASONING_AGENT_SPECS_DIR / self.AGENT_SPEC_FILE)
        return generate_agent_spec(template, TICKER=self.ticker)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Execute the final report compilation.

        Steps:
            1. Check cache (skip if output already exists and not forced)
            2. Compile all upstream outputs into a single dossier
            3. Upload the dossier to Gemini
            4. Generate the Markdown investment report
            5. Save the result and clean up remote files
        """
        output_path = self._build_output_path()

        if not self.force_resummarize and os.path.exists(output_path):
            logger.info(f"[FinalReportCompiler] Output already exists at {output_path}. Skipping.")
            return

        # 1. Compile all source documents into a single dossier
        logger.info(f"[FinalReportCompiler] Compiling dossier for {self.ticker}...")
        dossier_text = self._compile_dossier()
        logger.info(
            f"[FinalReportCompiler] Dossier compiled: {len(dossier_text):,} chars "
            f"from all upstream pipeline outputs."
        )

        # 2. Write dossier to temp file and upload
        file_manager = GeminiFileManager(self.client)
        ai_file: AIHostedFile | None = None

        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", encoding="utf-8", delete=False) as tmp:
            tmp.write(dossier_text)

            try:
                ai_file = await file_manager.upload_for_inference(tmp.name, "text/plain")

                # 3. Generate the final report
                agent_spec = self._build_agent_spec()
                extractor = GeminiExtractorAgent(self.client, GEMINI.final_report_model)
                report = await extractor.generate(
                    ai_file,
                    agent_spec,
                    temperature=GEMINI.final_report_temperature,
                    response_mime_type=ResponseTypes.TEXT.value,
                )

                # 4. Save the result
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(report)

                logger.info(f"[FinalReportCompiler] Report written to '{output_path}'")

            finally:
                # 5. Cleanup: remote upload
                if ai_file:
                    await file_manager.cleanup_remote_file(ai_file.file_name)
