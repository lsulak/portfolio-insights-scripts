"""Edgar Document Summarizer — AI-powered extraction of structured data from a single filing."""

import json
import logging
import os
from dataclasses import dataclass, field

from jinja2 import Template

from helios.config import GEMINI
from helios.extraction_engine.edgar.cleaner import EdgarDocumentCleaner
from helios.extraction_engine.edgar.domain import (
    AGENT_ADDITIONS_FIRST_10K_ONLY,
    EDGAR_FORMS_TO_CLEAN,
    SCHEMA_ADDITIONS_FIRST_10K_ONLY,
    EdgarFormType,
    LocalEdgarDocument,
)
from helios.utils.gemini_model_invoker import GeminiSingleInvoker
from helios.utils.gemini_file_manager import AIHostedFile, GeminiFileManager, ResponseTypes

logger = logging.getLogger(__name__)


@dataclass
class PreparedEdgarDocument:
    """Holds everything needed to include one filing in a Batch API request."""

    doc: LocalEdgarDocument
    output_file: str
    rendered_spec: str
    file_path_ai_ready: str
    ai_file: AIHostedFile | None = field(default=None, repr=False)

    def to_inline_request(self, temperature: float, response_mime_type: str) -> dict:
        """Build the inline Batch API request dict for this document."""
        if self.ai_file is None:
            raise ValueError("ai_file must be set before building a batch request")
        return {
            "contents": [
                {
                    "parts": [{"file_data": {"file_uri": self.ai_file.file_uri, "mime_type": self.ai_file.mime_type}}],
                    "role": "user",
                }
            ],
            "system_instruction": {"parts": [{"text": self.rendered_spec}]},
            "generation_config": {"temperature": temperature, "response_mime_type": response_mime_type},
        }


class EdgarDocumentSummarizer:
    """Summarizes a single Edgar filing via Gemini: clean → upload → extract → validate → save."""

    def __init__(self, file_manager: GeminiFileManager, gemini_model_invoker: GeminiSingleInvoker, cleaner: EdgarDocumentCleaner, summarized_dir: str):
        self._file_manager = file_manager
        self._invoker = gemini_model_invoker
        self._cleaner = cleaner
        self._summarized_dir = summarized_dir

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def summarize(
        self,
        doc: LocalEdgarDocument,
        agent_spec_template: Template,
        is_most_recent_of_its_type: bool,
        force_resummarize: bool,
    ) -> None:
        """Process a single Edgar document through the full summarization flow.

        Raises on failure.
        """
        output_file = self._output_path(doc)

        if self._is_cached(doc, output_file, force_resummarize):
            return

        rendered_spec = self._render_agent_spec(
            doc.form_type, agent_spec_template, doc.ticker, is_most_recent_of_its_type
        )
        doc.file_path_ai_ready = self._prepare_document(doc)

        raw_summary = await self._upload_and_extract(doc, rendered_spec)
        self._save_json(output_file, raw_summary)
        logger.info(f"Saved summary to: {output_file}")

    async def _upload_and_extract(self, doc: LocalEdgarDocument, rendered_spec: str) -> str:
        """Upload document to Gemini, run extraction, and clean up the remote file."""
        ai_file = None
        try:
            logger.info(
                f"Summarizing document for {doc.ticker}, {doc.form_type} "
                f"[{doc.submission_year} / {doc.submission_order_for_the_year}]"
            )
            ai_file = await self._file_manager.upload_for_inference(doc.file_path_ai_ready, doc.mime_type)
            return await self._invoker.generate(
                ai_file,
                rendered_spec,
                temperature=GEMINI.extractor_temperature,
                response_mime_type=ResponseTypes.JSON.value,
            )
        finally:
            if ai_file is not None:
                try:
                    await self._file_manager.cleanup_remote_file(ai_file.file_name)
                except Exception as cleanup_err:
                    logger.warning(f"Cleanup failed for {ai_file.file_name}: {cleanup_err}")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _output_path(self, doc: LocalEdgarDocument) -> str:
        filename = f"{doc.submission_year}_{doc.submission_order_for_the_year:06d}.json"
        return os.path.join(self._summarized_dir, doc.form_type, filename)

    @staticmethod
    def _is_cached(doc: LocalEdgarDocument, output_file: str, force_resummarize: bool) -> bool:
        """Return True (and log) if the summary already exists and re-run is not forced."""
        if os.path.exists(output_file) and not force_resummarize:
            logger.info(
                f"Summary exists for {doc.ticker}, {doc.form_type} "
                f"[{doc.submission_year} / {doc.submission_order_for_the_year}]. Skipping."
            )
            return True
        
        return False

    def _prepare_document(self, doc: LocalEdgarDocument) -> str:
        """Clean the document if its form type requires it, otherwise pass through."""
        if doc.form_type in EDGAR_FORMS_TO_CLEAN:
            return self._cleaner.clean_and_minify(doc, GEMINI.max_chars_per_document)
        
        logger.debug(f"Bypassing cleaner for {doc.file_path_raw}")
        return doc.file_path_raw

    @staticmethod
    def _render_agent_spec(form_type: EdgarFormType, base_spec: Template, ticker: str, is_most_recent: bool) -> str:
        """Render the Jinja2 agent spec, injecting extra context for the latest 10-K."""
        if form_type == EdgarFormType.ANNUAL_REPORT:
            business_and_risk_addition = AGENT_ADDITIONS_FIRST_10K_ONLY if is_most_recent else ""
            business_and_risk_schema = SCHEMA_ADDITIONS_FIRST_10K_ONLY if is_most_recent else ""

            if is_most_recent:
                logger.info(
                    f"Using enhanced specs for most recent {form_type} filing of {ticker} with business and risk factor context."
                )
            return base_spec.render(
                business_and_risk=business_and_risk_addition,
                business_and_risk_schema=business_and_risk_schema,
                TICKER=ticker,
            )
        return base_spec.render(TICKER=ticker)

    @staticmethod
    def _save_json(output_file: str, raw_response: str) -> None:
        """Validate, minify, and persist the AI response as JSON."""
        try:
            parsed = json.loads(raw_response)
        except json.JSONDecodeError as e:
            raise ValueError(f"AI returned invalid JSON: {e}") from e

        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(parsed, f, separators=(",", ":"), ensure_ascii=False)
