"""Stock Valuation Engine — deterministic DCF and relative valuation.

Consumes outputs from four prior synthesis/extraction stages:
    1. Quantitative Baseline  (YAML — financial triad ledger)
    2. Narrative Validation   (Markdown — forensic audit report)
    3. Sector Analysis        (Markdown — industry rivalry & peers)
    4. Market Analysis        (Markdown — macro cycle & pricing)

and feeds them as a single compiled dossier to a Gemini model with the
``stock_valuation.md`` agent spec.

The result is a Markdown valuation report containing intrinsic value,
relative valuation, reverse DCF, and margin of safety.
"""

import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Generator

from google import genai

from helios.config import GEMINI, SYNTHESIS_AGENT_SPECS_DIR, OutputDir
from helios.utils.commons import (
    AIHostedFile,
    GeminiExtractorAgent,
    GeminiFileManager,
    ResponseTypes,
    generate_agent_spec,
    load_agent_spec,
)

logger = logging.getLogger(__name__)


class StockValuationEngine:
    """Executes a Damodaran-style DCF valuation for a ticker.

    Gathers the quantitative baseline ledger, narrative validation report,
    sector analysis, and market analysis, then feeds them to a Gemini model
    with the stock valuation agent spec to produce a Markdown valuation report.
    """

    AGENT_SPEC_FILE = "stock_valuation.md"

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
        filename = f"valuation_{now.year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.STOCK_VALUATION, filename)

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

        Documents are ordered: quantitative baseline → narrative validation →
        sector analysis → market analysis.

        Raises:
            FileNotFoundError: If no source documents are found at all.
        """
        sections: list[str] = []

        # 1. Quantitative Baseline (YAML ledger)
        baseline_docs = self._collect_quantitative_baseline()
        if baseline_docs:
            sections.append("=" * 60)
            sections.append("QUANTITATIVE BASELINE PAYLOAD (YAML)")
            sections.append("=" * 60)
            for label, content in baseline_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 2. Narrative Validation (forensic audit)
        narrative_docs = self._collect_narrative_validation()
        if narrative_docs:
            sections.append("\n" + "=" * 60)
            sections.append("NARRATIVE VALIDATION PAYLOAD")
            sections.append("=" * 60)
            for label, content in narrative_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 3. Sector Analysis
        sector_docs = self._collect_sector_analysis()
        if sector_docs:
            sections.append("\n" + "=" * 60)
            sections.append("SECTOR ANALYSIS PAYLOAD")
            sections.append("=" * 60)
            for label, content in sector_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 4. Market Analysis
        market_docs = self._collect_market_analysis()
        if market_docs:
            sections.append("\n" + "=" * 60)
            sections.append("MARKET ANALYSIS PAYLOAD")
            sections.append("=" * 60)
            for label, content in market_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        if not sections:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. "
                f"Run the synthesis and extraction engines first."
            )

        return "\n".join(sections)

    # ------------------------------------------------------------------
    # Agent spec
    # ------------------------------------------------------------------

    def _build_agent_spec(self) -> str:
        """Load and render the stock valuation agent spec."""
        template = load_agent_spec(SYNTHESIS_AGENT_SPECS_DIR / self.AGENT_SPEC_FILE)
        return generate_agent_spec(template, TICKER=self.ticker)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Execute the stock valuation analysis.

        Steps:
            1. Check cache (skip if output already exists and not forced)
            2. Compile synthesis/extraction outputs into a single dossier
            3. Upload the dossier to Gemini
            4. Generate the Markdown valuation report
            5. Save the result and clean up remote files
        """
        output_path = self._build_output_path()

        if not self.force_resummarize and os.path.exists(output_path):
            logger.info(f"[StockValuationEngine] Output already exists at {output_path}. Skipping.")
            return

        # 1. Compile all source documents into a single dossier
        logger.info(f"[StockValuationEngine] Compiling dossier for {self.ticker}...")
        dossier_text = self._compile_dossier()
        logger.info(
            f"[StockValuationEngine] Dossier compiled: {len(dossier_text):,} chars "
            f"from synthesis and extraction outputs."
        )

        # 2. Write dossier to temp file and upload
        file_manager = GeminiFileManager(self.client)
        ai_file: AIHostedFile | None = None

        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", encoding="utf-8", delete=False) as tmp:
            tmp.write(dossier_text)

            try:
                ai_file = await file_manager.upload_for_inference(tmp.name, "text/plain")

                # 3. Generate the valuation report
                agent_spec = self._build_agent_spec()
                extractor = GeminiExtractorAgent(self.client, GEMINI.stock_valuation_model)
                report = await extractor.generate(
                    ai_file,
                    agent_spec,
                    temperature=GEMINI.stock_valuation_temperature,
                    response_mime_type=ResponseTypes.TEXT.value,
                )

                # 4. Save the result
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(report)

                logger.info(f"[StockValuationEngine] Valuation written to '{output_path}'")

            finally:
                # 5. Cleanup: remote upload
                if ai_file:
                    await file_manager.cleanup_remote_file(ai_file.file_name)
