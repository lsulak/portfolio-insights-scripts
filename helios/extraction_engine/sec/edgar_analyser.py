"""Edgar (SEC Filing) extraction pipeline - Orchestrates the complete extraction workflow."""

import asyncio
from dataclasses import dataclass
import json
import logging
import os
import traceback
from typing import Optional

from google import genai
from jinja2 import Template

from helios.config import GEMINI, SEC_EDGAR
from helios.extraction_engine.sec.api import LocalEdgarDocument, EdgarFormType
from helios.extraction_engine.sec.fetcher import EdgarFetcher
from helios.extraction_engine.sec.cleaner import EdgarDocumentCleaner
from helios.extraction_engine.sec.ai_summarizer import GeminiFileManager, ExtractorAgent
from helios.extraction_engine.sec.constants import (
    SEC_FORM_TO_AGENT_SPEC,
    SEC_FORM_TO_YEARS_BACK,
    SEC_FORMS_TO_CLEAN,
    AGENT_ADDITIONS_FIRST_10K_ONLY,
)

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
    """Manages the complete Edgar (SEC filing) extraction and summarization workflow."""

    def __init__(self, client: genai.Client, ticker: str, output_base_dir: str, force_resummarize: bool = False):
        """Initialize the Edgar extraction pipeline.

        Args:
            client: Gemini API client
            ticker: Stock ticker symbol
            output_base_dir: Base directory for output files
            force_resummarize: Whether to force re-summarization of existing files
        """
        self.client = client
        self.ticker = ticker
        self.output_base_dir = output_base_dir
        self.force_resummarize = force_resummarize

        # Directory structure
        self._setup_directories()

        # Initialize services (lazy loading)
        self._edgar_fetcher: Optional[EdgarFetcher] = None
        self._edgar_cleaner: Optional[EdgarDocumentCleaner] = None
        self._gemini_file_manager: Optional[GeminiFileManager] = None
        self._gemini_extractor: Optional[ExtractorAgent] = None

        self._llm_semaphore: Optional[asyncio.Semaphore] = None

    def _setup_directories(self) -> None:
        """Create required directory structure."""
        ticker_dir = os.path.join(self.output_base_dir, self.ticker)

        self.dir_raw = os.path.join(ticker_dir, "company_filings_raw")
        self.dir_minified = os.path.join(ticker_dir, "company_filings_minified")
        self.dir_summarized = os.path.join(ticker_dir, "company_filings_summarized")

        # Create base directories
        os.makedirs(self.dir_raw, exist_ok=True)

        # Create subdirectories for each form type
        for form_type in SEC_FORM_TO_AGENT_SPEC.keys():
            os.makedirs(os.path.join(self.dir_minified, form_type), exist_ok=True)
            os.makedirs(os.path.join(self.dir_summarized, form_type), exist_ok=True)

    def _initialize_services(self) -> None:
        """Initialize all required services."""
        self._edgar_fetcher = EdgarFetcher(
            company_name=SEC_EDGAR.company_name, email_address=SEC_EDGAR.email, download_dir=self.dir_raw
        )
        self._edgar_cleaner = EdgarDocumentCleaner(target_dir=self.dir_minified)
        self._gemini_file_manager = GeminiFileManager(self.client)
        self._gemini_extractor = ExtractorAgent(self.client, model_name=GEMINI.extractor_model)
        self._llm_semaphore = asyncio.Semaphore(GEMINI.max_parallel_calls)

    async def _process_document(
        self, doc: LocalEdgarDocument, agent_specs: Template, is_most_recent_of_its_type: bool
    ) -> bool:
        """Process a single SEC document through the extraction pipeline.

        Args:
            doc: Document to process
            agent_specs: Agent specification template
            is_most_recent_of_its_type: Whether this is the most recent filing of its type

        Returns:
            True if successful, False otherwise
        """
        summary_filename = f"{doc.submission_year}_{doc.submission_order_for_the_year}.json"
        output_file = os.path.join(self.dir_summarized, doc.form_type, summary_filename)

        # Early exit if summary exists and no force flag
        if os.path.exists(output_file) and not self.force_resummarize:
            logger.info(
                f"Summary exists for {doc.ticker} {doc.form_type} "
                f"[{doc.submission_year} / {doc.submission_order_for_the_year}]. Skipping."
            )
            return True

        current_specs = self._prepare_agent_specs(doc.form_type, agent_specs, is_most_recent_of_its_type)

        doc.file_path_ai_ready = self._prepare_document(doc)

        # Process with AI
        ai_file = None
        try:
            async with self._llm_semaphore:
                logger.info(
                    f"[GEMINI] Summarizing {doc.ticker} {doc.form_type} [{doc.submission_year} / {doc.submission_order_for_the_year}]"
                )
                ai_file = await self._gemini_file_manager.upload_for_inference(doc)
                structured_summary = await self._gemini_extractor.generate_structured_dossier(ai_file, current_specs)

            # Save result
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, "w", encoding="utf-8") as f:
                validated_json = self._validate_and_minify_json(structured_summary)
                f.write(validated_json)

            logger.info(f"✅ Saved summary to: {output_file}")
            return True

        except Exception as e:
            logger.error(
                f"❌ Failed {doc.ticker} {doc.form_type} [{doc.submission_year} / {doc.submission_order_for_the_year}]: "
                f"{type(e).__name__}: {e}"
            )
            if os.getenv("DEBUG"):
                logger.debug(traceback.format_exc())
            return False

        finally:
            if ai_file is not None:
                try:
                    await self._gemini_file_manager.cleanup_remote_file(ai_file.file_name)
                except Exception as cleanup_err:
                    logger.warning(f"Cleanup failed for {ai_file.file_name}: {cleanup_err}")

    @staticmethod
    def _validate_and_minify_json(raw_response: str) -> str:
        """Validate AI response is valid JSON and minify it.

        Raises:
            ValueError: If the response is not valid JSON.
        """
        try:
            parsed = json.loads(raw_response)
        except json.JSONDecodeError as e:
            raise ValueError(f"AI returned invalid JSON: {e}") from e
        return json.dumps(parsed, separators=(",", ":"), ensure_ascii=False)

    def _prepare_agent_specs(self, form_type: EdgarFormType, base_specs: Template, is_most_recent: bool) -> str:
        """Prepare agent specifications for extraction.

        Args:
            form_type: Type of SEC form
            base_specs: Base specification template
            is_most_recent: Whether this is the most recent filing

        Returns:
            Prepared specification string
        """
        if form_type == EdgarFormType.ANNUAL_REPORT:
            if is_most_recent:
                logger.info(f"Using enhanced specs for most recent {form_type}")
                return base_specs.render(business_and_risk=AGENT_ADDITIONS_FIRST_10K_ONLY)
            else:
                return base_specs.render(business_and_risk="")
        else:
            return base_specs.render()

    def _prepare_document(self, doc: LocalEdgarDocument) -> str:
        """Clean and prepare document for AI processing.

        Args:
            doc: Document to prepare

        Returns:
            Path to AI-ready document
        """
        if doc.form_type in SEC_FORMS_TO_CLEAN:
            return self._edgar_cleaner.clean_and_minify(doc, GEMINI.max_chars_per_document)
        else:
            logger.debug(f"Bypassing cleaner for {doc.file_path_raw}")
            return doc.file_path_raw

    async def _fetch_and_queue_documents(self) -> list:
        """Fetch documents from SEC and queue them for processing.

        Returns:
            List of processing tasks
        """
        tasks = []

        for form_type, agent_specs in SEC_FORM_TO_AGENT_SPEC.items():
            await asyncio.sleep(SEC_EDGAR.api_call_delay_seconds)

            years_back = SEC_FORM_TO_YEARS_BACK.get(form_type)
            if years_back is None:
                logger.warning(f"No years_back configured for {form_type}, skipping.")
                continue

            historical_docs = await self._edgar_fetcher.fetch_latest_filings(self.ticker, form_type, years_back)

            if not historical_docs:
                logger.info(f"No {form_type} documents found for {self.ticker}.")
                continue

            # Determine most recent filing
            is_annual = form_type == EdgarFormType.ANNUAL_REPORT
            last_year = max(doc.submission_year for doc in historical_docs) if is_annual else -1

            # Queue all documents for processing
            for doc in historical_docs:
                is_most_recent = is_annual and doc.submission_year == last_year
                task = self._process_document(doc, agent_specs, is_most_recent)
                tasks.append(task)

        return tasks

    async def run(self) -> EdgarExtractionResult:
        """Execute the complete extraction pipeline.

        Returns:
            EdgarExtractionResult with execution statistics
        """
        logger.info(f"🚀 Starting SEC Extraction Pipeline for {self.ticker}")

        self._initialize_services()

        extraction_tasks = await self._fetch_and_queue_documents()

        if not extraction_tasks:
            logger.warning(f"No documents to process for {self.ticker}")
            return EdgarExtractionResult(total=0, successful=0, failed=0)

        logger.info(f"🚀 Processing {len(extraction_tasks)} documents concurrently...")

        # Execute all tasks
        results = await asyncio.gather(*extraction_tasks)

        # Calculate statistics
        successful = sum(1 for r in results if r is True)
        failed = len(results) - successful

        logger.info(f"🎯 Pipeline complete for {self.ticker}")
        logger.info(f"✅ Successful: {successful}/{len(results)}")
        if failed > 0:
            logger.error(f"❌ Failed: {failed}/{len(results)}")

        return EdgarExtractionResult(
            total=len(results),
            successful=successful,
            failed=failed,
        )
