"""SEC Document Summarizer - AI-powered extraction of structured data from SEC filings."""

import asyncio
import logging

import google.api_core.exceptions as google_errors
from google import genai
from google.genai import types, errors
from tenacity import before_sleep_log, retry, wait_exponential, stop_after_attempt, retry_if_exception

from helios.extraction_engine.sec.api import IAIFileManager, IExtractorAgent, LocalEdgarDocument, AIHostedFile
from helios.config import GEMINI

logger = logging.getLogger(__name__)


class GeminiFileManager(IAIFileManager):
    """Handles the lifecycle of large documents in the Gemini Files API."""

    def __init__(self, client: genai.Client):
        self.client = client

    async def upload_for_inference(self, document: LocalEdgarDocument) -> AIHostedFile:
        """Uploads the file to Google's servers.

        Files uploaded here exist for 48 hours and cannot be downloaded back.
        """
        logger.info(f"[GEMINI] Uploading {document.ticker} {document.form_type} to AI...")

        # Native async: no thread pool required.
        uploaded_file = await self.client.aio.files.upload(file=document.file_path_ai_ready)

        # Wait for file processing with timeout
        file_info = await self.client.aio.files.get(name=uploaded_file.name)
        max_wait_time = 300  # 5 minutes timeout
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
            mime_type=document.mime_type,
        )

    async def cleanup_remote_file(self, file_name: str) -> bool:
        """Manually purges the document from Google servers."""
        try:
            await self.client.aio.files.delete(name=file_name)
            logger.debug(f"[GEMINI] Remote file {file_name} deleted.")
            return True
        except Exception as e:
            logger.warning(f"[GEMINI] Failed to delete remote file: {e}")
            return False


class ExtractorAgent(IExtractorAgent):
    """Executes the extraction prompt against the Gemini model."""

    def __init__(self, client: genai.Client, model_name: str):
        self.client = client
        self.model_name = model_name

    def list_available_models(self):
        logger.info("[GEMINI] Available models:")
        for model in self.client.models.list():
            logger.info(f"  - {model}")

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
        """Forces the model to act as a structured extractor."""
        logger.info(f"[GEMINI] Extracting financials from {ai_file.file_name}...")

        config = types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=GEMINI.extractor_temperature,
            response_mime_type="application/json",
        )

        # Grab the file reference asynchronously
        file_ref = await self.client.aio.files.get(name=ai_file.file_name)

        logger.info(f"[GEMINI] File {ai_file.file_name} received from the agent, gonna do the main magic now")

        # Native async generation
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
