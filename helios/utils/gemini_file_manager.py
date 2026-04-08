"""Gemini file management — upload, poll, and cleanup for the Gemini Files API."""

import asyncio
import enum
import logging
from dataclasses import dataclass

from google import genai

logger = logging.getLogger(__name__)


class ResponseTypes(enum.Enum):
    """Expected response formats from Gemini models."""

    JSON = "application/json"
    TEXT = "text/plain"


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
        logger.info(f"Uploading file {file_path} to Gemini storage...")

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

        logger.info(f"File ready in Gemini storage: {uploaded_file.uri}")

        return AIHostedFile(
            file_uri=uploaded_file.uri,
            file_name=uploaded_file.name,
            mime_type=mime_type,
        )

    async def cleanup_remote_file(self, file_name: str) -> None:
        """Manually purge a document from Gemini storage. Best-effort; logs on failure."""
        try:
            await self.client.aio.files.delete(name=file_name)
            logger.debug(f"Remote file {file_name} deleted from Gemini storage.")
        except Exception as e:
            logger.warning(f"Failed to delete remote file from Gemini storage: {e}")
