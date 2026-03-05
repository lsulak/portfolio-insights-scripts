"""Business Overview Synthetizer — structured executive primer on the business.

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


class BusinessOverviewSynthetizer:
    """Produces a structured business primer for a ticker.

    Gathers narrative validation, quantitative baseline, sector analysis,
    earnings call synthesis, and selective Edgar filings (all 8-Ks, all 10-Qs,
    last 10-K only), then feeds them to a Gemini model with the business
    overview agent spec to produce a Markdown executive primer.
    """

    AGENT_SPEC_FILE = "business_overview.md"

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
        filename = f"overview_{now.year}-Q{quarter}.md"
        return os.path.join(self._ticker_dir(), OutputDir.BUSINESS_OVERVIEW, filename)

    # ------------------------------------------------------------------
    # Document collection
    # ------------------------------------------------------------------

    def _collect_narrative_validation(self) -> list[tuple[str, str]]:
        """Read the narrative validation report(s) (Markdown)."""
        narrative_dir = os.path.join(self._ticker_dir(), OutputDir.NARRATIVE_VALIDATION)
        return list(self._read_dir_files(narrative_dir, "*.md"))

    def _collect_quantitative_baseline(self) -> list[tuple[str, str]]:
        """Read the quantitative baseline YAML ledger(s)."""
        baseline_dir = os.path.join(self._ticker_dir(), OutputDir.QUANTITATIVE_BASELINE)
        return list(self._read_dir_files(baseline_dir, "*.yaml"))

    def _collect_sector_analysis(self) -> list[tuple[str, str]]:
        """Read all sector analysis reports (Markdown)."""
        sector_dir = os.path.join(self._ticker_dir(), OutputDir.SECTOR_ANALYSIS)
        return list(self._read_dir_files(sector_dir, "*.md"))

    def _collect_earnings_calls(self) -> list[tuple[str, str]]:
        """Read all earnings call synthesis reports (Markdown)."""
        earnings_dir = os.path.join(self._ticker_dir(), OutputDir.EARNINGS_CALLS)
        return list(self._read_dir_files(earnings_dir, "*.md"))

    def _collect_edgar_filings(self) -> list[tuple[str, str]]:
        """Read selective Edgar filings: all 8-Ks, all 10-Qs, last 10-K only.

        Files are sorted by filename descending (latest first) within each
        form type group.
        """
        docs: list[tuple[str, str]] = []
        base = os.path.join(self._ticker_dir(), OutputDir.EDGAR_SUMMARIZED)

        # All 8-Ks
        self._append_all_filings(docs, base, EdgarFormType.CURRENT_REPORT)

        # All 10-Qs
        self._append_all_filings(docs, base, EdgarFormType.QUARTERLY_REPORT)

        # Only the last (most recent) 10-K
        self._append_latest_filing(docs, base, EdgarFormType.ANNUAL_REPORT)

        return docs

    def _append_all_filings(
        self, docs: list[tuple[str, str]], base: str, form_type: EdgarFormType
    ) -> None:
        """Append all filings of a given form type, latest first."""
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

    def _append_latest_filing(
        self, docs: list[tuple[str, str]], base: str, form_type: EdgarFormType
    ) -> None:
        """Append only the most recent filing of a given form type."""
        form_dir = os.path.join(base, form_type)
        if not os.path.isdir(form_dir):
            raise Exception(f"Expected Edgar summarized directory not found: {form_dir}.")

        files = sorted(Path(form_dir).glob("*.json"), reverse=True)
        if not files:
            logger.warning(f"No {form_type} filings found in {form_dir}.")
            return

        fp = files[0]
        label = f"EDGAR {form_type}/{fp.name} (latest only)"
        content = fp.read_text(encoding="utf-8")
        if len(content) == 0:
            raise Exception(f"Empty content in {label}.")
        docs.append((label, content))

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

        Documents are ordered: narrative validation → quantitative baseline →
        sector analysis → earnings calls → Edgar filings (8-K, 10-Q, last 10-K).

        Raises:
            FileNotFoundError: If no source documents are found at all.
        """
        sections: list[str] = []

        # 1. Narrative Validation (forensic audit)
        narrative_docs = self._collect_narrative_validation()
        if narrative_docs:
            sections.append("=" * 60)
            sections.append("NARRATIVE VALIDATION PAYLOAD")
            sections.append("=" * 60)
            for label, content in narrative_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 2. Quantitative Baseline (YAML ledger — segment data)
        baseline_docs = self._collect_quantitative_baseline()
        if baseline_docs:
            sections.append("\n" + "=" * 60)
            sections.append("QUANTITATIVE BASELINE PAYLOAD (YAML)")
            sections.append("=" * 60)
            for label, content in baseline_docs:
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

        # 4. Earnings Call Synthesis
        earnings_docs = self._collect_earnings_calls()
        if earnings_docs:
            sections.append("\n" + "=" * 60)
            sections.append("EARNINGS CALL SYNTHESIS")
            sections.append("=" * 60)
            for label, content in earnings_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        # 5. Edgar Filings (all 8-Ks, all 10-Qs, last 10-K only)
        edgar_docs = self._collect_edgar_filings()
        if edgar_docs:
            sections.append("\n" + "=" * 60)
            sections.append("EDGAR FILINGS (8-K all, 10-Q all, 10-K latest only)")
            sections.append("=" * 60)
            for label, content in edgar_docs:
                sections.append(f"\n--- {label} ---\n")
                sections.append(content)

        if not sections:
            raise FileNotFoundError(
                f"No source documents found for {self.ticker}. "
                f"Run the extraction and synthesis engines first."
            )

        return "\n".join(sections)

    # ------------------------------------------------------------------
    # Agent spec
    # ------------------------------------------------------------------

    def _build_agent_spec(self) -> str:
        """Load and render the business overview agent spec."""
        template = load_agent_spec(SYNTHESIS_AGENT_SPECS_DIR / self.AGENT_SPEC_FILE)
        return generate_agent_spec(template, TICKER=self.ticker)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Execute the business overview synthesis.

        Steps:
            1. Check cache (skip if output already exists and not forced)
            2. Compile synthesis/extraction outputs into a single dossier
            3. Upload the dossier to Gemini
            4. Generate the Markdown business overview
            5. Save the result and clean up remote files
        """
        output_path = self._build_output_path()

        if not self.force_resummarize and os.path.exists(output_path):
            logger.info(f"[BusinessOverviewSynthetizer] Output already exists at {output_path}. Skipping.")
            return

        # 1. Compile all source documents into a single dossier
        logger.info(f"[BusinessOverviewSynthetizer] Compiling dossier for {self.ticker}...")
        dossier_text = self._compile_dossier()
        logger.info(
            f"[BusinessOverviewSynthetizer] Dossier compiled: {len(dossier_text):,} chars "
            f"from synthesis and extraction outputs."
        )

        # 2. Write dossier to temp file and upload
        file_manager = GeminiFileManager(self.client)
        ai_file: AIHostedFile | None = None

        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", encoding="utf-8", delete=False) as tmp:
            tmp.write(dossier_text)

            try:
                ai_file = await file_manager.upload_for_inference(tmp.name, "text/plain")

                # 3. Generate the business overview
                agent_spec = self._build_agent_spec()
                extractor = GeminiExtractorAgent(self.client, GEMINI.business_overview_model)
                report = await extractor.generate(
                    ai_file,
                    agent_spec,
                    temperature=GEMINI.business_overview_temperature,
                    response_mime_type=ResponseTypes.TEXT.value,
                )

                # 4. Save the result
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(report)

                logger.info(f"[BusinessOverviewSynthetizer] Overview written to '{output_path}'")

            finally:
                # 5. Cleanup: remote upload
                if ai_file:
                    await file_manager.cleanup_remote_file(ai_file.file_name)
