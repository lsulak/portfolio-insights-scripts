"""Quantitative Baseline Compiler — deterministic financial data aggregator.

Consumes outputs from two extraction engine analysers:
    1. Earnings Calls     (Markdown — forward guidance numbers only)
    2. Edgar Filings      (JSON, grouped by form type, latest first)

and feeds them as a single compiled dossier to a Gemini model with the
``quantitative_baseline_compiler.md`` agent spec.

The result is a YAML ledger containing the complete financial triad.
"""

import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Generator

from google import genai

from helios.config import GEMINI, SYNTHESIS_AGENT_SPECS_DIR, OutputDir
from helios.extraction_engine.edgar.domain import EdgarFormType
from helios.utils.commons import (
    AIHostedFile,
    GeminiExtractorAgent,
    GeminiFileManager,
    ResponseTypes,
    generate_agent_spec,
    load_agent_spec,
)

logger = logging.getLogger(__name__)


class QuantitativeBaselineCompiler:
    """Compiles a deterministic YAML financial ledger for a ticker.

    Gathers earnings call synthesis and summarized Edgar filings, then feeds
    them to a Gemini model with the quantitative baseline compiler agent spec
    to produce a chronologically ordered YAML ledger.
    """

    AGENT_SPEC_FILE = "quantitative_baseline.md"

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
        filename = f"ledger_{now.year}-Q{quarter}.yaml"
        return os.path.join(self._ticker_dir(), OutputDir.QUANTITATIVE_BASELINE, filename)

    # ------------------------------------------------------------------
    # Document collection
    # ------------------------------------------------------------------

    def _collect_earnings_calls(self) -> list[tuple[str, str]]:
        """Read all earnings call synthesis reports (Markdown)."""
        earnings_dir = os.path.join(self._ticker_dir(), OutputDir.EARNINGS_CALLS)
        return list(self._read_dir_files(earnings_dir, "*.md"))

    def _collect_edgar_filings(self) -> list[tuple[str, str]]:
        """Read all summarized Edgar filings grouped by form type, latest first.

        Form types are iterated in ``EdgarFormType`` enum order (10-K first,
        most analytically important).  Within each group, files are sorted by
        filename descending so the most recent filings appear first.
        """
        docs: list[tuple[str, str]] = []
        base = os.path.join(self._ticker_dir(), OutputDir.EDGAR_SUMMARIZED)

        for form_type in EdgarFormType:
            form_dir = os.path.join(base, form_type)
            if not os.path.isdir(form_dir):
                raise Exception(f"Expected Edgar summarized directory not found: {form_dir}.")

            files = sorted(Path(form_dir).glob("*.json"), reverse=True)
            for fp in files:
                label = f"EDGAR {form_type}/{fp.name}"
                content = fp.read_text(encoding="utf-8")
                if len(content) == 0:
                    raise Exception(f"Empty content in {label}.")

                docs.append((label, content))

        return docs

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

        Documents are ordered: earnings calls → Edgar filings
        (within each group, latest first).

        Raises:
            FileNotFoundError: If no source documents are found at all.
        """
        sections: list[str] = []

        # 1. Earnings Call Synthesis (forward guidance extraction)
        earnings_docs = self._collect_earnings_calls()
        if earnings_docs:
            sections.append("=" * 60)
            sections.append("EARNINGS CALL SYNTHESIS")
            sections.append("=" * 60)
            for label, content in earnings_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 2. Edgar Filings (grouped by form type, latest first within each)
        edgar_docs = self._collect_edgar_filings()
        if edgar_docs:
            sections.append("\n" + "=" * 60)
            sections.append("EDGAR FILINGS (latest first)")
            sections.append("=" * 60)
            for label, content in edgar_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        if not sections:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. "
                f"Run the extraction engine first."
            )

        return "\n".join(sections)

    # ------------------------------------------------------------------
    # Agent spec
    # ------------------------------------------------------------------

    def _build_agent_spec(self) -> str:
        """Load and render the quantitative baseline compiler agent spec."""
        template = load_agent_spec(SYNTHESIS_AGENT_SPECS_DIR / self.AGENT_SPEC_FILE)
        return generate_agent_spec(template, TICKER=self.ticker)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Execute the quantitative baseline compilation.

        Steps:
            1. Check cache (skip if output already exists and not forced)
            2. Compile earnings calls and Edgar outputs into a single dossier
            3. Upload the dossier to Gemini
            4. Generate the YAML financial ledger
            5. Save the result and clean up remote files
        """
        output_path = self._build_output_path()

        if not self.force_resummarize and os.path.exists(output_path):
            logger.info(f"[QuantitativeBaselineCompiler] Output already exists at {output_path}. Skipping.")
            return

        # 1. Compile all source documents into a single dossier
        logger.info(f"[QuantitativeBaselineCompiler] Compiling dossier for {self.ticker}...")
        dossier_text = self._compile_dossier()
        logger.info(
            f"[QuantitativeBaselineCompiler] Dossier compiled: {len(dossier_text):,} chars "
            f"from extraction engine outputs."
        )

        # 2. Write dossier to temp file and upload
        file_manager = GeminiFileManager(self.client)
        ai_file: AIHostedFile | None = None

        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", encoding="utf-8", delete=False) as tmp:
            tmp.write(dossier_text)

            try:
                ai_file = await file_manager.upload_for_inference(tmp.name, "text/plain")

                # 3. Generate the YAML financial ledger
                agent_spec = self._build_agent_spec()
                extractor = GeminiExtractorAgent(self.client, GEMINI.quantitative_baseline_model)
                report = await extractor.generate(
                    ai_file,
                    agent_spec,
                    temperature=GEMINI.quantitative_baseline_temperature,
                    response_mime_type=ResponseTypes.TEXT.value,
                )

                # 4. Save the result
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(report)

                logger.info(f"[QuantitativeBaselineCompiler] Ledger written to '{output_path}'")

            finally:
                # 5. Cleanup: remote upload
                if ai_file:
                    await file_manager.cleanup_remote_file(ai_file.file_name)
