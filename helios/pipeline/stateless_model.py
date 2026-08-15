"""Stateless model analyser — base class for direct LLM call workflows.

These analysers gather documents produced by earlier pipeline layers, bundle
them into a single text file, upload it to the AI platform, and run a
structured extraction / synthesis call against it.

The model call is stateless — no agent memory, no tool use, no internet
access. The LLM receives the full context in a single request and returns
a single response.
"""

import logging
import os
import tempfile
from abc import abstractmethod

from google import genai

from helios.pipeline.base import BaseAnalyser
from helios.utils.agent_spec import AgentSpecRenderer
from helios.utils.gemini_model_invoker import GeminiSingleInvoker
from helios.utils.gemini_file_manager import AIHostedFile, GeminiFileManager, ResponseTypes

logger = logging.getLogger(__name__)


class StatelessModelAnalyser(BaseAnalyser):
    """Template-method base for analysers that compile upstream pipeline outputs
    into a single document, upload it to the AI platform, and run a stateless
    model call for extraction or synthesis.

    Subclasses must set class attributes:
        AGENT_SPEC_FILE  – filename of the agent spec template

    And implement the abstract methods:
        _get_model()         – model identifier
        _get_temperature()   – generation temperature
        _build_output_path() – filesystem path for the output
        _compile_sources()   – concatenated source documents
    """

    AGENT_SPEC_FILE: str

    def __init__(
        self,
        client: genai.Client,
        file_manager: GeminiFileManager,
        spec_renderer: AgentSpecRenderer,
        output_base_dir: str,
        ticker: str,
        force_recompute: bool = False,
    ) -> None:
        super().__init__(output_base_dir, ticker, force_recompute)
        self.client = client
        self._file_manager = file_manager
        self.spec_renderer = spec_renderer

    # ------------------------------------------------------------------
    # Agent spec
    # ------------------------------------------------------------------

    def _build_agent_spec(self) -> str:
        """Render the agent spec with the ticker.

        Override in subclasses that need additional template variables.
        """
        return self.spec_renderer.render(self.AGENT_SPEC_FILE, TICKER=self.ticker)

    # ------------------------------------------------------------------
    # Abstract methods
    # ------------------------------------------------------------------

    @abstractmethod
    def _get_model(self) -> str:
        """Return the model identifier for this analyser."""

    @abstractmethod
    def _get_temperature(self) -> float:
        """Return the generation temperature for this analyser."""

    @abstractmethod
    def _compile_sources(self) -> str:
        """Compile all upstream outputs into a single source document."""

    # ------------------------------------------------------------------
    # Template method
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Compile sources, upload to the AI platform, generate report, and persist."""
        output_path = self._build_output_path()

        if self._is_cached(output_path):
            return

        logger.info(f"Compiling sources for {self.ticker}...")
        source_text = self._compile_sources()
        logger.info(f"Sources for model compiled: {len(source_text):,} chars.")

        ai_file: AIHostedFile | None = None
        tmp_path: str | None = None

        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".md", encoding="utf-8", delete=False) as tmp:
                tmp_path = tmp.name
                tmp.write(source_text)

            ai_file = await self._file_manager.upload_for_inference(tmp_path, "text/plain")

            agent_spec = self._build_agent_spec()
            invoker = GeminiSingleInvoker(self.client, self._get_model())
            report = await invoker.generate(
                ai_file,
                agent_spec,
                temperature=self._get_temperature(),
                response_mime_type=ResponseTypes.TEXT.value,
            )

            self._persist(output_path, report)

        finally:
            if ai_file:
                await self._file_manager.cleanup_remote_file(ai_file.file_name)
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
