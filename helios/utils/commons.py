import asyncio
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import google.api_core.exceptions as google_errors
from google import genai
from google.genai import types, errors
from jinja2 import Template
from tenacity import before_sleep_log, retry, wait_exponential, stop_after_attempt, retry_if_exception

from helios.config import AGENT_SPECS_DIR, GEMINI

logger = logging.getLogger(__name__)


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
    async def generate_structured_dossier(self, ai_file: AIHostedFile, system_prompt: str) -> str:
        """Run a structured extraction prompt against the Gemini model.

        Args:
            ai_file: Handle returned by ``GeminiFileManager.upload_for_inference``.
            system_prompt: The rendered agent spec / system instruction.

        Returns:
            Raw JSON string from the model.
        """
        logger.info(f"[GEMINI] Extracting from {ai_file.file_name}...")

        config = types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=GEMINI.extractor_temperature,
            response_mime_type="application/json",
        )

        file_ref = await self.client.aio.files.get(name=ai_file.file_name)

        response = await self.client.aio.models.generate_content(
            model=self.model_name,
            contents=[file_ref, "Extract the required data according to the system instructions."],
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
    """

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

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _current_quarter() -> tuple[str, int]:
        """Return ``(year_str, quarter_number)`` for the current date."""
        now = datetime.now()
        quarter = ((now.month - 1) // 3) + 1
        return str(now.year), quarter

    def _ticker_output_dir(self, subdir: str) -> str:
        """Return ``<output_base_dir>/<ticker>/<subdir>``."""
        return os.path.join(self.output_base_dir, self.ticker, subdir)

    @staticmethod
    def _render_agent_spec(spec_filename: str, **keywords: str) -> str:
        """Load an agent spec by filename and render it with the given keywords.

        Convenience wrapper combining ``load_agent_spec`` + ``generate_agent_spec``.
        Looks up the file under ``AGENT_SPECS_DIR``.
        """
        template = load_agent_spec(AGENT_SPECS_DIR / spec_filename)
        return generate_agent_spec(template, **keywords)

    # ------------------------------------------------------------------
    # Deep Research polling
    # ------------------------------------------------------------------

    async def _poll(self, agent_spec: str, polling_interval: int = 60, max_wait_minutes: int = 30) -> str:
        """Execute a Gemini Deep Research agent and poll until completion.

        Args:
            agent_spec: The rendered agent specification prompt
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

        interaction = self.client.interactions.create(agent=model, input=agent_spec, background=True)
        interaction_id = interaction.id
        logger.info(f"[Interaction ID: {interaction_id}] - Agent dispatched. Entering polling loop...")

        max_iterations = (max_wait_minutes * 60) // polling_interval
        for iteration in range(max_iterations):
            current_state = self.client.interactions.get(id=interaction_id)
            status = current_state.status

            if status == "completed":
                logger.info("--- RESEARCH COMPLETED ---")
                logger.debug(f"Final Agent Output:\n{current_state.outputs}\n")
                return current_state.outputs[-1].text

            elif status in ("failed", "cancelled"):
                raise RuntimeError(f"Agent execution failed with status: {status}")

            else:
                elapsed = (iteration + 1) * polling_interval
                logger.info(
                    f"Agent Status: [{status.upper()}] - still crawling and synthesizing. "
                    f"Waiting {polling_interval}s... (elapsed: {elapsed // 60}m {elapsed % 60}s)"
                )
                await asyncio.sleep(polling_interval)

        raise TimeoutError(
            f"Deep Research agent did not complete within {max_wait_minutes} minutes "
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
        result = await self._poll(agent_spec)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result)

        logger.info(f"[{label}] Report written to '{output_path}'")
