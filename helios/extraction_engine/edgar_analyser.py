"""Edgar extraction pipeline — high-level orchestrator.

Coordinates the building blocks in the edgar/ sub-package:
  fetcher  → download filings from Edgar
  cleaner  → strip binary/HTML bloat
  summarizer → AI-powered structured extraction per document
"""

import asyncio
import logging
import os

from pathlib import Path

from google import genai

from helios.config import GEMINI, EDGAR, EXTRACTION_FORCE, OutputDir
from helios.extraction_engine.edgar.cleaner import EdgarDocumentCleaner
from helios.extraction_engine.edgar.domain import (
    EdgarFormType,
    EDGAR_FORM_TO_AGENT_SPEC,
    EDGAR_FORM_TO_YEARS_BACK,
)
from helios.extraction_engine.edgar.fetcher import EdgarFetcher
from helios.extraction_engine.edgar.summarizer import EdgarDocumentSummarizer
from helios.pipeline.base import BaseAnalyser
from helios.utils.gemini_file_manager import GeminiFileManager
from helios.utils.gemini_model_invoker import GeminiSingleInvoker

logger = logging.getLogger(__name__)


class EdgarExtractionPipeline(BaseAnalyser):
    """Fetches, cleans, and summarizes Edgar filings for a given ticker.

    Extends ``BaseAnalyser`` directly — it has its own multi-phase
    workflow and does not use agent specs or the stateless-model template.
    """

    def __init__(
        self,
        client: genai.Client,
        file_manager: GeminiFileManager,
        output_base_dir: str,
        ticker: str,
        force_recompute: bool = False,
    ):
        self.force_recompute = force_recompute or EXTRACTION_FORCE.edgar

        super().__init__(output_base_dir, ticker, self.force_recompute)
        self._client = client
        self._file_manager = file_manager
        
        ticker_dir = os.path.join(output_base_dir, ticker)
        self._dir_raw = os.path.join(ticker_dir, OutputDir.EDGAR_RAW)
        self._dir_minified = os.path.join(ticker_dir, OutputDir.EDGAR_MINIFIED)
        self._dir_summarized = os.path.join(ticker_dir, OutputDir.EDGAR_SUMMARIZED)

    def _build_output_path(self) -> str:
        return self._dir_summarized

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Execute the complete extraction pipeline."""
        logger.info(f"Starting Edgar Extraction Pipeline for {self.ticker}")

        self._ensure_directories()

        if not self.force_recompute and not self._is_dir_empty(self._dir_raw):
            logger.info(
                f"Raw EDGAR data directory {self._dir_raw} is not empty or force_recompute is False. "
                f"Skipping fetching and summarization."
            )
            return

        fetcher = EdgarFetcher(company_name=EDGAR.company_name, email_address=EDGAR.email, download_dir=self._dir_raw)
        summarizer = EdgarDocumentSummarizer(
            file_manager=self._file_manager,
            gemini_model_invoker=GeminiSingleInvoker(client=self._client, model_name=GEMINI.edgar_extractor_model),
            cleaner=EdgarDocumentCleaner(target_dir=self._dir_minified),
            summarized_dir=self._dir_summarized,
        )

        docs_to_summarize = await self._fetch_all_filings(fetcher)

        if not docs_to_summarize:
            logger.warning(f"No documents to process for {self.ticker}")
            return

        semaphore = asyncio.Semaphore(GEMINI.max_parallel_calls)

        async def _bounded_summarize(doc, spec_template, is_most_recent):
            async with semaphore:
                await summarizer.summarize(doc, spec_template, is_most_recent, self.force_recompute)

        await asyncio.gather(*(
            _bounded_summarize(doc, spec_template, is_most_recent)
            for doc, spec_template, is_most_recent in docs_to_summarize
        ))

        logger.info(f"Edgar pipeline complete: {len(docs_to_summarize)} documents processed.")

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _is_dir_empty(self, directory: str) -> bool:
        """Check if a directory contains any files (recursively)."""
        return not any(p.is_file() for p in Path(directory).rglob("*"))

    def _ensure_directories(self) -> None:
        os.makedirs(self._dir_raw, exist_ok=True)
        for form_type in EDGAR_FORM_TO_AGENT_SPEC:
            os.makedirs(os.path.join(self._dir_minified, form_type), exist_ok=True)
            os.makedirs(os.path.join(self._dir_summarized, form_type), exist_ok=True)

    async def _fetch_all_filings(self, fetcher: EdgarFetcher) -> list[tuple]:
        """Fetch filings for every configured form type.

        Returns:
            List of ``(doc, agent_spec_template, is_most_recent)`` tuples.
        """
        results = []

        for form_type, agent_spec_template in EDGAR_FORM_TO_AGENT_SPEC.items():
            await asyncio.sleep(EDGAR.api_call_delay_seconds)

            years_back = EDGAR_FORM_TO_YEARS_BACK.get(form_type)
            if years_back is None:
                logger.warning(f"No years_back configured for {form_type}, skipping.")
                continue

            docs = await fetcher.fetch_latest_filings(self.ticker, form_type, years_back)
            if not docs:
                logger.info(f"No {form_type} documents found for {self.ticker}.")
                continue

            is_annual = form_type == EdgarFormType.ANNUAL_REPORT
            if is_annual:
                latest_doc = max(docs, key=lambda d: (d.submission_year, d.submission_order_for_the_year))
            else:
                latest_doc = None

            for doc in docs:
                is_most_recent = is_annual and doc is latest_doc
                results.append((doc, agent_spec_template, is_most_recent))

        logger.info(f"Fetched {len(results)} documents for summarization.")
        return results
