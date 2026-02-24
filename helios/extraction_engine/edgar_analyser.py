"""Edgar extraction pipeline — high-level orchestrator.

Coordinates the building blocks in the edgar/ sub-package:
  fetcher  → download filings from Edgar
  cleaner  → strip binary/HTML bloat
  summarizer → AI-powered structured extraction per document
"""

import asyncio
from dataclasses import dataclass
import logging
import os

from google import genai

from helios.config import GEMINI, EDGAR
from helios.extraction_engine.edgar.cleaner import EdgarDocumentCleaner
from helios.extraction_engine.edgar.domain import (
    EdgarFormType,
    EDGAR_FORM_TO_AGENT_SPEC,
    EDGAR_FORM_TO_YEARS_BACK,
)
from helios.extraction_engine.edgar.fetcher import EdgarFetcher
from helios.extraction_engine.edgar.summarizer import EdgarDocumentSummarizer

logger = logging.getLogger(__name__)


@dataclass
class EdgarExtractionResult:
    """Outcome of an extraction pipeline run."""

    total: int
    successful: int
    failed: int

    @property
    def all_passed(self) -> bool:
        return self.failed == 0


class EdgarExtractionPipeline:
    """Fetches, cleans, and summarizes Edgar filings for a given ticker."""

    def __init__(self, client: genai.Client, output_base_dir: str, ticker: str, force_resummarize: bool = False):
        self.client = client
        self.ticker = ticker
        self.force_resummarize = force_resummarize

        ticker_dir = os.path.join(output_base_dir, ticker)
        self._dir_raw = os.path.join(ticker_dir, "company_filings_raw")
        self._dir_minified = os.path.join(ticker_dir, "company_filings_minified")
        self._dir_summarized = os.path.join(ticker_dir, "company_filings_summarized")

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def run(self) -> EdgarExtractionResult:
        """Execute the complete extraction pipeline."""
        logger.info(f"🚀 Starting Edgar Extraction Pipeline for {self.ticker}")

        self._ensure_directories()

        fetcher = EdgarFetcher(
            company_name=EDGAR.company_name, email_address=EDGAR.email, download_dir=self._dir_raw
        )
        summarizer = EdgarDocumentSummarizer(
            client=self.client,
            summarized_dir=self._dir_summarized,
            cleaner=EdgarDocumentCleaner(target_dir=self._dir_minified),
        )
        semaphore = asyncio.Semaphore(GEMINI.max_parallel_calls)

        tasks = await self._build_tasks(fetcher, summarizer, semaphore)

        if not tasks:
            logger.warning(f"No documents to process for {self.ticker}")
            return EdgarExtractionResult(total=0, successful=0, failed=0)

        logger.info(f"🚀 Processing {len(tasks)} documents concurrently...")
        results = await asyncio.gather(*tasks)

        successful = sum(1 for r in results if r is True)
        failed = len(results) - successful

        logger.info(f"🎯 Pipeline complete: ✅ {successful} / ❌ {failed}")
        return EdgarExtractionResult(total=len(results), successful=successful, failed=failed)

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _ensure_directories(self) -> None:
        os.makedirs(self._dir_raw, exist_ok=True)
        for form_type in EDGAR_FORM_TO_AGENT_SPEC:
            os.makedirs(os.path.join(self._dir_minified, form_type), exist_ok=True)
            os.makedirs(os.path.join(self._dir_summarized, form_type), exist_ok=True)

    async def _build_tasks(
        self, fetcher: EdgarFetcher, summarizer: EdgarDocumentSummarizer, semaphore: asyncio.Semaphore
    ) -> list:
        """Fetch filings for every configured form type and return summarization tasks."""
        tasks = []

        for form_type, agent_spec in EDGAR_FORM_TO_AGENT_SPEC.items():
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
            latest_year = max(d.submission_year for d in docs) if is_annual else -1

            for doc in docs:
                is_most_recent = is_annual and doc.submission_year == latest_year

                async def _bounded_summarize(d=doc, spec=agent_spec, recent=is_most_recent):
                    async with semaphore:
                        return await summarizer.summarize(d, spec, recent, self.force_resummarize)

                tasks.append(_bounded_summarize())

        return tasks

