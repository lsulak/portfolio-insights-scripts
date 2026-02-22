import argparse
import asyncio
import logging
import os
import sys
import traceback
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from helios.extraction_engine.edgar_extraction import LocalSECDocument, SECFetcher, GeminiFileManager, ExtractorAgent
from helios.utils.SECExtractor import SECExtractor
from helios.utils.constants import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/helios_extraction.log"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def process_document(
    doc: LocalSECDocument,
    output_reports_summarized_dir: str,
    is_most_recent_of_its_type: bool,
    base_specs,
    file_manager,
    extractor,
    heuristic_sec_cleaner,
    llm_semaphore: asyncio.Semaphore,
    force_resummarize: bool = False,
) -> bool:
    """Handles the extraction lifecycle for a single SEC filing concurrently.

    Returns:
       bool: True if successful, False otherwise
    """
    summary_filename = f"{doc.submission_year}_{doc.submission_order_for_the_year}.json"
    output_file = os.path.join(output_reports_summarized_dir, doc.form_type, summary_filename)

    # Early exit if summary exists and no force flag
    if os.path.exists(output_file) and not force_resummarize:
        logger.info(f"Summary already exists for {doc.ticker}, form '{doc.form_type}'. Skipping.")
        return True

    if doc.form_type == "10-K":
        if is_most_recent_of_its_type:
            logger.info(
                f"Using enhanced agent specs for {doc.ticker} most recent {doc.form_type} ({doc.submission_year})"
            )
            current_specs = base_specs.substitute(business_and_risk=AGENT_ADDITIONS_FIRST_10K_ONLY)
        else:
            current_specs = base_specs.substitute(business_and_risk="")
    else:
        current_specs = base_specs.substitute()

    if doc.form_type in EDGAR_REPORT_TYPES_TO_MINIMIZE:
        doc.file_path_ai_ready = heuristic_sec_cleaner.clean_and_minify(doc, MAX_CHARS_PER_DOCUMENT)
    else:
        logger.debug(f"Bypassing heuristic cleaner for {doc.file_path_raw}")
        doc.file_path_ai_ready = doc.file_path_raw

    # UnboundLocalError Fix: Pre-allocate the variable outside the try block
    ai_file = None

    try:
        # Bounded Concurrency: The semaphore ensures we never exceed the specified API limits
        async with llm_semaphore:
            logger.info(
                f"[AI] Summarizing {doc.ticker}, form '{doc.form_type}' for {doc.submission_year} [{doc.submission_order_for_the_year}]"
            )
            ai_file = await file_manager.upload_for_inference(doc)
            structured_summary = await extractor.generate_structured_dossier(ai_file, current_specs)

        # Ensure directory exists and write file
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(structured_summary)

        logger.info(f"✅ AI summary saved locally to: {output_file}")
        return True

    except Exception as e:
        # Isolates the failure so it does not crash the rest of the batch
        logger.error(
            f"❌ Failed on {doc.ticker} {doc.form_type} {doc.submission_year} [{doc.submission_order_for_the_year}]: "
            f"{type(e).__name__}: {e}"
        )
        if os.getenv("DEBUG"):
            logger.debug(traceback.format_exc())
        return False

    finally:
        # Safely clean up only if the upload actually succeeded
        if ai_file is not None:
            try:
                await file_manager.cleanup_remote_file(ai_file.file_name)
            except Exception as cleanup_err:
                logger.warning(f"Failed to clean up remote file {ai_file.file_name}: {cleanup_err}")


async def run_extraction_pipeline(ticker: str, force_resummarize: bool = False):
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)

    logger.info(f"🚀 Starting HELIOS Extraction Pipeline for {ticker}")

    # Setup directory structure
    _output_dir = os.path.join("data", "helios", ticker)
    output_dir_company_filings_raw = os.path.join(_output_dir, "company_filings_raw")
    output_dir_company_filings_minified = os.path.join(_output_dir, "company_filings_minified")
    output_dir_company_fillings_summarized = os.path.join(_output_dir, "company_filings_summarized")

    # Create base directories
    os.makedirs(_output_dir, exist_ok=True)
    os.makedirs(output_dir_company_filings_raw, exist_ok=True)

    # Create subdirectories for each form type
    for form_type in MAP_EDGAR_REPORT_TYPE_TO_AGENT_SPEC.keys():
        os.makedirs(os.path.join(output_dir_company_filings_minified, form_type), exist_ok=True)
        os.makedirs(os.path.join(output_dir_company_fillings_summarized, form_type), exist_ok=True)

    # Initialize services
    fetcher = SECFetcher(
        company_name=MY_COMPANY_NAME, email_address=MY_EMAIL, download_dir=output_dir_company_filings_raw
    )
    heuristic_sec_cleaner = SECExtractor(target_dir=output_dir_company_filings_minified)
    file_manager = GeminiFileManager(api_key=GEMINI_API_KEY)
    extractor = ExtractorAgent(api_key=GEMINI_API_KEY, model_name=EXTRACTOR_MODEL)

    # extractor.list_available_models()

    # Set the upper bound for parallel LLM requests
    llm_semaphore = asyncio.Semaphore(MAX_LLM_PARALLEL_CALLS)

    extraction_tasks = []

    for form_type, agent_specs in MAP_EDGAR_REPORT_TYPE_TO_AGENT_SPEC.items():
        await asyncio.sleep(TIMEOUT_BETWEEN_EDGAR_API_CALLS)

        years_back = MAP_EDGAR_REPORT_TYPE_TO_YEARS_BACK.get(form_type)
        if years_back is None:
            logger.warning(f"No years_back configured for {form_type}, skipping.")
            continue

        historical_docs = await fetcher.fetch_latest_filings(ticker, form_type, years_back)
        if not historical_docs:
            logger.info(f"No {form_type} documents found for {ticker}.")
            continue

        is_10k = form_type == "10-K"
        last_year_of_10k_submission = max(doc.submission_year for doc in historical_docs) if is_10k else -1

        for doc in historical_docs:
            is_most_recent_of_its_type = is_10k and doc.submission_year == last_year_of_10k_submission

            # Queue the document extraction logic as an independent asynchronous task
            task = process_document(
                doc=doc,
                output_reports_summarized_dir=output_dir_company_fillings_summarized,
                is_most_recent_of_its_type=is_most_recent_of_its_type,
                base_specs=agent_specs,
                file_manager=file_manager,
                extractor=extractor,
                heuristic_sec_cleaner=heuristic_sec_cleaner,
                force_resummarize=force_resummarize,
                llm_semaphore=llm_semaphore,
            )
            extraction_tasks.append(task)

    logger.info(f"\n🚀 Deploying {len(extraction_tasks)} document extraction tasks concurrently...")

    # Execute all queued tasks concurrently while letting the semaphore throttle the network
    extraction_tasks_results = await asyncio.gather(*extraction_tasks, return_exceptions=True)

    # Summary statistics
    successful = sum(1 for r in extraction_tasks_results if r is True)
    failed = len(extraction_tasks_results) - successful

    logger.info(f"\n🎯 HELIOS Pipeline execution complete for {ticker}.")
    logger.info(f"   ✅ Successful: {successful}/{len(extraction_tasks_results)}")
    if failed > 0:
        logger.error(f"   ❌ Failed: {failed}/{len(extraction_tasks_results)}")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Run the HELIOS Extraction Pipeline for a specified ticker.")
    arg_parser.add_argument(
        "--ticker", type=str, default=TESTING_TICKER, help="The ticker symbol to extract filings for (default: GOOGL)."
    )
    arg_parser.add_argument(
        "--force-resummarize",
        action="store_true",
        help="If set, forces re-summarization of all documents even if summaries already exist.",
    )
    args = arg_parser.parse_args()

    asyncio.run(run_extraction_pipeline(args.ticker, force_resummarize=args.force_resummarize))
