"""Edgar Document Summarizer — AI-powered extraction of structured data from a single filing."""

import json
import logging
import os
import traceback

from google import genai
from jinja2 import Template

from helios.config import GEMINI
from helios.extraction_engine.edgar.cleaner import EdgarDocumentCleaner
from helios.extraction_engine.edgar.domain import (
    AGENT_ADDITIONS_FIRST_10K_ONLY,
    EDGAR_FORMS_TO_CLEAN,
    EdgarFormType,
    LocalEdgarDocument,
)
from helios.utils.commons import GeminiExtractorAgent, GeminiFileManager

logger = logging.getLogger(__name__)


class EdgarDocumentSummarizer:
    """Summarizes a single Edgar filing via Gemini: clean → upload → extract → validate → save."""

    def __init__(self, client: genai.Client, summarized_dir: str, cleaner: EdgarDocumentCleaner):
        self._file_manager = GeminiFileManager(client)
        self._extractor = GeminiExtractorAgent(client, model_name=GEMINI.extractor_model)
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
    ) -> bool:
        """Process a single Edgar document through the full summarization flow.

        Returns:
            True if successful (or already cached), False on failure.
        """
        output_file = self._output_path(doc)

        if os.path.exists(output_file) and not force_resummarize:
            logger.info(
                f"Summary exists for {doc.ticker} {doc.form_type} "
                f"[{doc.submission_year} / {doc.submission_order_for_the_year}]. Skipping."
            )
            return True

        rendered_spec = self._render_agent_spec(doc.form_type, agent_spec_template, doc.ticker, is_most_recent_of_its_type)
        doc.file_path_ai_ready = self._prepare_document(doc)

        ai_file = None
        try:
            logger.info(
                f"[GEMINI] Summarizing {doc.ticker} {doc.form_type} "
                f"[{doc.submission_year} / {doc.submission_order_for_the_year}]"
            )
            ai_file = await self._file_manager.upload_for_inference(doc.file_path_ai_ready, doc.mime_type)
            raw_summary = await self._extractor.generate_structured_dossier(ai_file, rendered_spec)

            self._save_json(output_file, raw_summary)
            logger.info(f"✅ Saved summary to: {output_file}")
            return True

        except Exception as e:
            logger.error(
                f"❌ Failed {doc.ticker} {doc.form_type} "
                f"[{doc.submission_year} / {doc.submission_order_for_the_year}]: "
                f"{type(e).__name__}: {e}"
            )
            if os.getenv("DEBUG"):
                logger.debug(traceback.format_exc())
            return False

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
            addition = AGENT_ADDITIONS_FIRST_10K_ONLY if is_most_recent else ""
            if is_most_recent:
                logger.info(f"Using enhanced specs for most recent {form_type} filing of {ticker} with business and risk factor context.")
            return base_spec.render(business_and_risk=addition, ticker=ticker)
        return base_spec.render(ticker=ticker)

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
