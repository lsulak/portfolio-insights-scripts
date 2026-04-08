"""Gemini model invokers — single-request and batch wrappers for the Gemini API.

Hierarchy::

    GeminiModelInvoker (base)   — shared client, model, retry classification
    ├── GeminiSingleInvoker     — one file → one model response (retry-aware)
    └── GeminiBatchInvoker      — N inline requests → N responses via Batch API
"""

import asyncio
import logging

import google.api_core.exceptions as google_errors
from google import genai
from google.genai import types, errors
from tenacity import before_sleep_log, retry, wait_exponential, stop_after_attempt, retry_if_exception

from helios.config import GEMINI
from helios.utils.gemini_file_manager import AIHostedFile, ResponseTypes

logger = logging.getLogger(__name__)

_BATCH_TERMINAL_STATES = {"JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"}


class GeminiModelInvoker:
    """Base for Gemini model call wrappers.

    Holds the shared ``client`` and ``model_name``, and provides a common
    retry-classification helper for transient Gemini API errors.
    """

    def __init__(self, client: genai.Client, model_name: str) -> None:
        self.client = client
        self.model_name = model_name

    @staticmethod
    def _is_retryable(exception: BaseException) -> bool:
        """Return True for transient errors that warrant a retry."""
        if isinstance(exception, (errors.ServerError, ConnectionError, TimeoutError)):
            return True
        if isinstance(exception, google_errors.GoogleAPICallError):
            return True
        if isinstance(exception, errors.ClientError):
            return getattr(exception, "code", None) in (408, 429)
        return False


class GeminiSingleInvoker(GeminiModelInvoker):
    """Resilient, retry-aware wrapper for a single Gemini generation call."""

    @retry(
        retry=retry_if_exception(GeminiModelInvoker._is_retryable),
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
        """Generate content from a single uploaded file.

        Args:
            ai_file: Handle returned by ``GeminiFileManager.upload_for_inference``.
            system_prompt: The rendered agent spec / system instruction.
            temperature: Sampling temperature.
            response_mime_type: Constrains the output format.

        Returns:
            Raw model output string.
        """
        logger.info(f"Submitting file {ai_file.file_name} (model={self.model_name})...")

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


class GeminiBatchInvoker(GeminiModelInvoker):
    """Submits and polls Gemini Batch API jobs for cost-efficient bulk generation.

    Uses inline requests — each request carries its own system instruction,
    generation config, and a reference to an already-uploaded file.
    """

    async def run_batch(
        self,
        inline_requests: list[dict],
        display_name: str = "helios-batch",
    ) -> list[str]:
        """Submit an inline batch job, poll until complete, return response texts.

        Args:
            inline_requests: List of dicts, each a valid ``GenerateContentRequest``
                             (with ``contents``, ``system_instruction``, ``generation_config``).
            display_name: Human-readable label for the job.

        Returns:
            List parallel to *inline_requests* — the model's text output per request.

        Raises:
            TimeoutError: If the job exceeds ``GEMINI.batch_max_wait_seconds``.
            RuntimeError: If the job ends in a non-success state or any
                          individual request fails.
        """
        logger.info(
            f"Submitting {len(inline_requests)} requests using batch mode "
            f"(model={self.model_name}, display_name={display_name})"
        )

        job = await asyncio.to_thread(
            self.client.batches.create,
            model=self.model_name,
            src=inline_requests,
            config={"display_name": display_name},
        )

        job_name = job.name
        logger.info(f"Batch job created: {job_name} (state: {job.state})")

        elapsed = 0
        while job.state.name not in _BATCH_TERMINAL_STATES:
            await asyncio.sleep(GEMINI.batch_poll_interval_seconds)
            elapsed += GEMINI.batch_poll_interval_seconds

            if elapsed > GEMINI.batch_max_wait_seconds:
                logger.warning(f"Cancelling {job_name} after {elapsed}s timeout")
                await asyncio.to_thread(self.client.batches.cancel, name=job_name)
                raise TimeoutError(f"Batch job {job_name} timed out after {elapsed}s")

            job = await asyncio.to_thread(self.client.batches.get, name=job_name)
            logger.info(f"Batch job {job_name}: {job.state.name} ({elapsed}s elapsed)")

        if job.state.name != "JOB_STATE_SUCCEEDED":
            raise RuntimeError(f"Batch job {job_name} ended with state {job.state.name}")

        results: list[str] = []
        for i, resp in enumerate(job.dest.inlined_responses):
            try:
                results.append(resp.response.text)
            except Exception as e:
                raise RuntimeError(f"Batch job {job_name}: request {i} returned no usable response: {e}") from e

        logger.info(f"Batch job {job_name} done: {len(results)}/{len(results)} succeeded")
        return results
