import asyncio
import enum
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import google.api_core.exceptions as google_errors
from google import genai
from google.genai import types, errors
from jinja2 import Template
from tenacity import before_sleep_log, retry, wait_exponential, stop_after_attempt, retry_if_exception

from helios.config import EXTRACTION_AGENT_SPECS_DIR, GEMINI, OutputDir

logger = logging.getLogger(__name__)


class ResponseTypes(enum.Enum):
    """Enum for expected response formats from Gemini agents."""

    JSON = "application/json"
    TEXT = "text/plain"


# ------------------------------------------------------------------
# Agent spec helpers
# ------------------------------------------------------------------


def load_agent_spec(spec_path: Path | str) -> Template:
    """Load a Markdown agent spec and compile it into a Jinja2 template.

    The Markdown file is the prompt — no intermediate parsing needed.
    Jinja2 variables (e.g. {{ business_and_risk }}) are resolved at render time.
    """
    logger.debug(f"Loading agent spec from {spec_path}...")

    with open(spec_path, "r", encoding="utf-8") as f:
        return Template(f.read())


def generate_agent_spec(spec_template: Template, **substitution_keywords: str) -> str:
    """Render a Jinja2 agent spec template with the given variables."""
    return spec_template.render(**substitution_keywords)


# ------------------------------------------------------------------
# General-purpose utilities
# ------------------------------------------------------------------


def current_quarter() -> tuple[str, int]:
    """Return ``(year_str, quarter_number)`` for the current date."""
    now = datetime.now()
    quarter = ((now.month - 1) // 3) + 1
    return str(now.year), quarter


def read_dir_files(directory: str, pattern: str) -> list[tuple[str, str]]:
    """Read all files matching *pattern* in *directory*, sorted descending by name.

    Raises:
        Exception: If the directory does not exist or any matched file is empty.
    """
    if not os.path.isdir(directory):
        raise Exception(f"Directory not found: {directory}")

    files = sorted(Path(directory).glob(pattern), reverse=True)
    results: list[tuple[str, str]] = []
    for fp in files:
        if fp.stat().st_size == 0:
            raise Exception(f"Empty content in: {fp}.")
        results.append((fp.name, fp.read_text(encoding="utf-8")))
    return results


# ------------------------------------------------------------------
# Gemini platform infrastructure
# ------------------------------------------------------------------


@dataclass
class AIHostedFile:
    """Reference to a file uploaded to Gemini's servers for inference."""

    file_uri: str
    file_name: str
    mime_type: str


class GeminiFileManager:
    """Handles the lifecycle of large documents in the Gemini Files API."""

    def __init__(self, client: genai.Client) -> None:
        self.client = client

    async def upload_for_inference(self, file_path: str, mime_type: str) -> AIHostedFile:
        """Upload a local file to Gemini's servers and wait until it is ready.

        Files uploaded here exist for 48 hours and cannot be downloaded back.

        Args:
            file_path: Local filesystem path of the file to upload.
            mime_type: MIME type of the file (e.g. ``text/plain``).

        Returns:
            An ``AIHostedFile`` handle that can be passed to an extractor.
        """
        logger.info(f"[GEMINI] Uploading {file_path} to AI...")

        uploaded_file = await self.client.aio.files.upload(file=file_path)

        # Wait for server-side processing with timeout
        file_info = await self.client.aio.files.get(name=uploaded_file.name)
        max_wait_time = 300  # 5 minutes
        elapsed = 0

        while file_info.state.name == "PROCESSING" and elapsed < max_wait_time:
            await asyncio.sleep(2)
            elapsed += 2
            file_info = await self.client.aio.files.get(name=uploaded_file.name)

        if file_info.state.name == "PROCESSING":
            raise TimeoutError(f"File processing timed out after {max_wait_time}s for {uploaded_file.name}")
        if file_info.state.name == "FAILED":
            raise ValueError(f"File processing failed for {uploaded_file.name}")

        logger.info(f"[GEMINI] File ready: {uploaded_file.uri}")

        return AIHostedFile(
            file_uri=uploaded_file.uri,
            file_name=uploaded_file.name,
            mime_type=mime_type,
        )

    async def cleanup_remote_file(self, file_name: str) -> bool:
        """Manually purge a document from Google servers."""
        try:
            await self.client.aio.files.delete(name=file_name)
            logger.debug(f"[GEMINI] Remote file {file_name} deleted.")
            return True
        except Exception as e:
            logger.warning(f"[GEMINI] Failed to delete remote file: {e}")
            return False


class GeminiExtractorAgent:
    """Resilient, retry-aware wrapper for Gemini structured extraction."""

    def __init__(self, client: genai.Client, model_name: str) -> None:
        self.client = client
        self.model_name = model_name

    @staticmethod
    def _is_retryable(exception: BaseException) -> bool:
        """Only retry on transient errors, not permanent client errors like 400/403."""
        if isinstance(exception, (errors.ServerError, ConnectionError, TimeoutError)):
            return True
        if isinstance(exception, google_errors.GoogleAPICallError):
            return True
        if isinstance(exception, errors.ClientError):
            # Only retry rate-limit (429) and request-timeout (408)
            return getattr(exception, "code", None) in (408, 429)
        return False

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(
            multiplier=4,
            min=GEMINI.retry_min_wait_seconds,
            max=GEMINI.retry_max_wait_seconds,
        ),
        stop=stop_after_attempt(GEMINI.max_retries),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def generate(
        self,
        ai_file: AIHostedFile,
        system_prompt: str,
        *,
        temperature: float = 0.0,
        response_mime_type: str = ResponseTypes.TEXT.value,
    ) -> str:
        """Core generation loop.

        Args:
            ai_file: Handle returned by ``GeminiFileManager.upload_for_inference``.
            system_prompt: The rendered agent spec / system instruction.
            response_mime_type: If set, constrains the output format.

        Returns:
            Raw model output string.
        """
        logger.info(f"[GEMINI] Processing {ai_file.file_name}...")

        config = types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=temperature,
            response_mime_type=response_mime_type,
        )

        file_ref = await self.client.aio.files.get(name=ai_file.file_name)

        response = await self.client.aio.models.generate_content(
            model=self.model_name,
            contents=[file_ref],
            config=config,
        )

        if response.text is None:
            raise ValueError(
                f"Gemini returned empty response for {ai_file.file_name}. "
                f"Possible safety filter or content policy block."
            )

        return response.text


# ------------------------------------------------------------------
# Deep Research base class
# ------------------------------------------------------------------


class DeepResearchAnalyser(ABC):
    """Template-method base for Gemini Deep Research Agent analysers.

    All Deep Research analysers (earnings calls, market, sector) follow the same
    workflow: build output path → check cache → render agent spec → poll until
    done → write result.  This base class captures that shared skeleton.

    Subclasses only provide the three pieces that vary:
        _get_model()          – which Gemini model to use
        _build_output_path()  – where to write the result
        _build_agent_spec()   – the rendered prompt

    Subclasses may override the class attribute:
        AGENT_SPECS_DIR       – directory containing agent spec files
    """

    AGENT_SPECS_DIR: Path = EXTRACTION_AGENT_SPECS_DIR

    def __init__(
        self, client: genai.Client, output_base_dir: str, ticker: str, force_resummarize: bool = False
    ) -> None:
        self.client = client
        self.output_base_dir = output_base_dir
        self.ticker = ticker
        self.force_resummarize = force_resummarize

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    @abstractmethod
    def _get_model(self) -> str:
        """Return the Gemini model identifier for this analyser."""

    @abstractmethod
    def _build_output_path(self) -> str:
        """Return the full filesystem path for the output report."""

    @abstractmethod
    def _build_agent_spec(self) -> str:
        """Return the fully-rendered agent spec prompt."""

    def _build_context(self) -> str:
        """Return optional upstream context to append to the agent spec input.

        Override in subclasses that need to feed prior pipeline outputs
        (e.g. Business Overview, Narrative Validation) into the Deep Research
        agent alongside the prompt.

        Returns:
            A string of compiled context.
        """
        return ""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _current_quarter() -> tuple[str, int]:
        """Return ``(year_str, quarter_number)`` for the current date."""
        return current_quarter()

    def _ticker_output_dir(self, subdir: str) -> str:
        """Return ``<output_base_dir>/<ticker>/<subdir>``."""
        return os.path.join(self.output_base_dir, self.ticker, subdir)

    def _render_agent_spec(self, spec_filename: str, **keywords: str) -> str:
        """Load an agent spec by filename and render it with the given keywords.

        Convenience wrapper combining ``load_agent_spec`` + ``generate_agent_spec``.
        Looks up the file under ``self.AGENT_SPECS_DIR``.
        """
        template = load_agent_spec(self.AGENT_SPECS_DIR / spec_filename)
        return generate_agent_spec(template, **keywords)

    # ------------------------------------------------------------------
    # Deep Research polling
    # ------------------------------------------------------------------

    async def _poll(self, agent_spec: str, label: str, polling_interval: int = 60, max_wait_minutes: int = 45) -> str:
        """Execute a Gemini Deep Research agent and poll until completion.

        Args:
            agent_spec: The rendered agent specification prompt
            label: The label for logging purposes
            polling_interval: Seconds between status checks
            max_wait_minutes: Maximum total wait time before raising TimeoutError

        Returns:
            The final synthesized research text

        Raises:
            RuntimeError: If the agent execution fails or is cancelled
            TimeoutError: If the agent does not complete within max_wait_minutes
        """
        model = self._get_model()
        logger.info("Deploying Gemini Deep Research Agent via Interactions API...")

        context = self._build_context()
        full_input = f"{agent_spec}\n\n{context}"

        interaction = self.client.interactions.create(agent=model, input=full_input, background=True)
        interaction_id = interaction.id
        logger.info(f"[Interaction ID: {interaction_id}] - Agent dispatched. Entering polling loop...")

        max_iterations = (max_wait_minutes * 60) // polling_interval
        for iteration in range(max_iterations):
            current_state = self.client.interactions.get(id=interaction_id)
            status = current_state.status

            if status == "completed":
                logger.info(f"[{label}] --- RESEARCH COMPLETED ---")
                logger.debug(f"[{label}] Final Agent Output:\n{current_state.outputs}\n")
                return current_state.outputs[-1].text

            elif status in ("failed", "cancelled"):
                raise RuntimeError(f"[{label}] Agent execution failed with status: {status}")

            else:
                elapsed = (iteration + 1) * polling_interval
                logger.info(
                    f"[{label}] Agent Status: [{status.upper()}] - still crawling and synthesizing. "
                    f"Waiting {polling_interval}s... (elapsed: {elapsed // 60}m {elapsed % 60}s)"
                )
                await asyncio.sleep(polling_interval)

        raise TimeoutError(
            f"[{label}] Deep Research agent did not complete within {max_wait_minutes} minutes "
            f"(interaction_id: {interaction_id})"
        )

    # ------------------------------------------------------------------
    # Template method
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Execute the Deep Research Agent and persist the result."""
        label = type(self).__name__
        output_path = self._build_output_path()

        if not self.force_resummarize and os.path.exists(output_path):
            logger.info(f"[{label}] Output already exists at {output_path}. Skipping.")
            return

        agent_spec = self._build_agent_spec()
        result = await self._poll(agent_spec, label)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result)

        logger.info(f"[{label}] Report written to '{output_path}'")


# ------------------------------------------------------------------
# Dossier-based analyser base class
# ------------------------------------------------------------------


class DossierAnalyser(ABC):
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
    AGENT_SPECS_DIR: Path

    def __init__(
        self, client: genai.Client, output_base_dir: str, ticker: str, force_resummarize: bool = False
    ) -> None:
        self.client = client
        self.output_base_dir = output_base_dir
        self.ticker = ticker
        self.force_resummarize = force_resummarize

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    @abstractmethod
    def _get_model(self) -> str:
        """Return the Gemini model identifier for this analyser."""

    @abstractmethod
    def _get_temperature(self) -> float:
        """Return the generation temperature for this analyser."""

    @abstractmethod
    def _build_output_path(self) -> str:
        """Return the full filesystem path for the output report."""

    @abstractmethod
    def _compile_dossier(self) -> str:
        """Compile all upstream outputs into a single dossier string."""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _ticker_dir(self) -> str:
        """Return ``<output_base_dir>/<ticker>``."""
        return os.path.join(self.output_base_dir, self.ticker)

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

    @staticmethod
    def _add_dossier_section(sections: list[str], heading: str, docs: list[tuple[str, str]]) -> None:
        """Append a labeled document group to the dossier sections list.

        Handles first-vs-subsequent section separator formatting automatically.
        """
        if not docs:
            return
        separator = "=" * 60
        prefix = "" if not sections else "\n"
        sections.append(prefix + separator)
        sections.append(heading)
        sections.append(separator)
        for label, content in docs:
            sections.append(f"\n--- {label} ---\n")
            sections.append(content)

    def _build_agent_spec(self) -> str:
        """Load and render the agent spec template with the ticker.

        Default implementation uses ``AGENT_SPECS_DIR / AGENT_SPEC_FILE``.
        Override if custom template variables are needed.
        """
        template = load_agent_spec(self.AGENT_SPECS_DIR / self.AGENT_SPEC_FILE)
        return generate_agent_spec(template, TICKER=self.ticker)

    # ------------------------------------------------------------------
    # Template method
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Compile dossier, upload to Gemini, generate report, and persist."""
        label = type(self).__name__
        output_path = self._build_output_path()

        if not self.force_resummarize and os.path.exists(output_path):
            logger.info(f"[{label}] Output already exists at {output_path}. Skipping.")
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

            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(report)

            logger.info(f"[{label}] Report written to '{output_path}'")

        finally:
            if ai_file:
                await file_manager.cleanup_remote_file(ai_file.file_name)
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
