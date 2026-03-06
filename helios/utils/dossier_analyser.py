"""Dossier analyser — base class for agents that compile upstream outputs.

These agents gather documents produced by earlier pipeline layers, bundle
them into a single "dossier" text file, upload it to Gemini, and run a
structured extraction / synthesis agent against it.
"""

import logging
import os
import tempfile
from abc import abstractmethod
from pathlib import Path

from helios.config import OutputDir
from helios.utils.base_analyser import BaseAnalyser
from helios.utils.commons import read_dir_files, ResponseTypes
from helios.utils.gemini_client import AIHostedFile, GeminiExtractorAgent, GeminiFileManager

logger = logging.getLogger(__name__)


class DossierAnalyser(BaseAnalyser):
    """Template-method base for analysers that compile a dossier from upstream
    pipeline outputs, upload it to Gemini, and run an extraction agent.

    Subclasses must set class attributes:
        AGENT_SPEC_FILE  – filename of the agent spec template
        AGENT_SPECS_DIR  – directory containing the spec file

    And implement the abstract methods:
        _get_model()         – Gemini model identifier
        _get_temperature()   – generation temperature
        _build_output_path() – filesystem path for the output
        _compile_dossier()   – concatenated source documents
    """

    AGENT_SPEC_FILE: str

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    @abstractmethod
    def _get_temperature(self) -> float:
        """Return the generation temperature for this analyser."""

    @abstractmethod
    def _compile_dossier(self) -> str:
        """Compile all upstream outputs into a single dossier string."""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _collect_files(self, output_subdir: str, pattern: str) -> list[tuple[str, str]]:
        """Read all files matching *pattern* from ``<ticker_dir>/<output_subdir>``."""
        directory = os.path.join(self._ticker_dir(), output_subdir)
        return read_dir_files(directory, pattern)

    def _collect_edgar_filings(self, form_types=None, max_per_type=None):
        """Read summarized Edgar filings, optionally limiting per form type.

        Args:
            form_types: Iterable of ``EdgarFormType`` values to include.
                        Defaults to all form types.
            max_per_type: Dict mapping ``EdgarFormType`` to maximum number of
                          filings to include. ``None`` means unlimited.

        Returns:
            List of ``(label, content)`` tuples.
        """
        from helios.extraction_engine.edgar.domain import EdgarFormType

        docs: list[tuple[str, str]] = []
        base = os.path.join(self._ticker_dir(), OutputDir.EDGAR_SUMMARIZED)
        types_to_collect = form_types if form_types is not None else list(EdgarFormType)

        for form_type in types_to_collect:
            form_dir = os.path.join(base, form_type)
            if not os.path.isdir(form_dir):
                raise Exception(f"Expected Edgar summarized directory not found: {form_dir}.")

            files = sorted(Path(form_dir).glob("*.json"), reverse=True)
            limit = (max_per_type or {}).get(form_type)
            if limit is not None:
                files = files[:limit]

            for fp in files:
                label = f"EDGAR {form_type}/{fp.name}"
                if limit == 1:
                    label += " (latest only)"
                content = fp.read_text(encoding="utf-8")
                if len(content) == 0:
                    raise Exception(f"Empty content in {label}.")
                docs.append((label, content))

        return docs

    def _build_agent_spec(self) -> str:
        """Load and render the agent spec template with the ticker.

        Default implementation uses ``AGENT_SPECS_DIR / AGENT_SPEC_FILE``.
        Override if custom template variables are needed.
        """
        return self._render_agent_spec(self.AGENT_SPEC_FILE, TICKER=self.ticker)

    # ------------------------------------------------------------------
    # Template method
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Compile dossier, upload to Gemini, generate report, and persist."""
        label = type(self).__name__
        output_path = self._build_output_path()

        if self._is_cached(output_path):
            return

        logger.info(f"[{label}] Compiling dossier for {self.ticker}...")
        dossier_text = self._compile_dossier()
        logger.info(f"[{label}] Dossier compiled: {len(dossier_text):,} chars.")

        file_manager = GeminiFileManager(self.client)
        ai_file: AIHostedFile | None = None
        tmp_path: str | None = None

        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".md", encoding="utf-8", delete=False) as tmp:
                tmp_path = tmp.name
                tmp.write(dossier_text)

            ai_file = await file_manager.upload_for_inference(tmp_path, "text/plain")

            agent_spec = self._build_agent_spec()
            extractor = GeminiExtractorAgent(self.client, self._get_model())
            report = await extractor.generate(
                ai_file,
                agent_spec,
                temperature=self._get_temperature(),
                response_mime_type=ResponseTypes.TEXT.value,
            )

            self._persist(output_path, report)

        finally:
            if ai_file:
                await file_manager.cleanup_remote_file(ai_file.file_name)
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
