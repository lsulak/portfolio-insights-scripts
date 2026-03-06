"""Gemini platform infrastructure — file management, retry-aware generation."""

import asyncio
import logging
from dataclasses import dataclass

import google.api_core.exceptions as google_errors
from google import genai
from google.genai import types, errors
from tenacity import before_sleep_log, retry, wait_exponential, stop_after_attempt, retry_if_exception

from helios.config import GEMINI
from helios.utils.commons import ResponseTypes

logger = logging.getLogger(__name__)


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
