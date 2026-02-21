import asyncio
from datetime import time
import os
from helios.extraction_engine.edgar_extraction import EDGARFetcher, GeminiFileManager, ExtractorAgent
from helios.utils.SECExtractor import SECExtractor
from helios.utils.constants import *


async def process_document(
    idx: int,
    doc,
    form_type: str,
    ticker: str,
    output_dir: str,
    is_most_recent_of_its_type: bool,
    base_specs,
    file_manager,
    extractor,
    heuristic_sec_cleaner,
    llm_semaphore: asyncio.Semaphore
):
    """Handles the extraction lifecycle for a single SEC filing concurrently."""
    
    current_specs = base_specs
    if form_type == "10-K":
        if is_most_recent_of_its_type:
            print(f"Using enhanced agent specs for most recent 10-K: {doc.local_file_path}")
            current_specs = base_specs.substitute(business_and_risk=AGENT_ADDITIONS_FIRST_10K_ONLY)
        else:
            current_specs = base_specs.substitute(business_and_risk="")
    else:
        current_specs = base_specs.substitute()

    if form_type in EDGAR_REPORT_TYPES_TO_MINIMIZE:
        doc.local_file_path = heuristic_sec_cleaner.heuristic_sec_cleaner(doc.local_file_path, MAX_CHARS_PER_DOCUMENT)
    else:
        print(f"   ⚡ [ROUTER] Bypassing heuristic cleaner for file {doc.local_file_path}...")

    # UnboundLocalError Fix: Pre-allocate the variable outside the try block
    ai_file = None 
    
    try:
        # 4. Bounded Concurrency: The semaphore ensures we never exceed the specified API limits
        async with llm_semaphore:
            print(f"Uploading and extracting [{form_type}] for ticker {ticker}: {idx+1}...")
            ai_file = await file_manager.upload_for_inference(doc)
            structured_summary = await extractor.generate_structured_dossier(ai_file, current_specs)

        output_file = os.path.join(output_dir, f"{form_type}_{idx}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(structured_summary)
        print(f"✅ AI summary saved locally to: {output_file}")

    except Exception as e:
        # Isolates the failure so it does not crash the rest of the batch
        print(f"❌ [ERROR] Failed on [{form_type}] for ticker {ticker}: {idx+1}: {e}")

    finally:
        # Safely clean up only if the upload actually succeeded
        if ai_file:
            try:
                await file_manager.cleanup_remote_file(ai_file.file_name)
            except Exception as cleanup_err:
                print(f"⚠️ [WARNING] Failed to clean up remote file {ai_file.file_name}: {cleanup_err}")


async def run_extraction_pipeline(ticker: str):
    print(f"Starting HELIOS Extraction Pipeline for {ticker}")

    _output_dir = os.path.join("data", "helios", ticker)
    output_dir_company_filings = os.path.join(_output_dir, "company_filings")
    output_dir_company_fillings_summarized = os.path.join(_output_dir, "company_filings_summarized")
    os.makedirs(_output_dir, exist_ok=True)
    os.makedirs(output_dir_company_filings, exist_ok=True)
    os.makedirs(output_dir_company_fillings_summarized, exist_ok=True)

    fetcher = EDGARFetcher(company_name=MY_COMPANY_NAME, email_address=MY_EMAIL, download_dir=output_dir_company_filings)
    file_manager = GeminiFileManager(api_key=GEMINI_API_KEY)
    extractor = ExtractorAgent(api_key=GEMINI_API_KEY, model_name=EXTRACTOR_MODEL)
    heuristic_sec_cleaner = SECExtractor()
    
    # extractor.list_available_models()

    # Set the upper bound for parallel LLM requests
    llm_semaphore = asyncio.Semaphore(MAX_LLM_PARALLEL_CALLS)
    
    extraction_tasks = []

    for form_type, agent_specs in MAP_EDGAR_REPORT_TYPE_TO_AGENT_SPEC.items():
        await asyncio.sleep(TIMEOUT_BETWEEN_EDGAR_API_CALLS) 
        
        years_back = MAP_EDGAR_REPORT_TYPE_TO_YEARS_BACK.get(form_type)
        historical_docs = await fetcher.fetch_latest_filings(ticker, form_type, years_back)
        if not historical_docs:
            print(f"Extraction Skipped: No documents found for {form_type}.")
            continue

        last_year_of_10k_submission = -1
        if form_type == "10-K":
            last_year_of_10k_submission = max([doc.submission_year for doc in historical_docs])

        for idx, doc in enumerate(historical_docs):
            is_most_recent_of_its_type = (form_type == "10-K" and doc.submission_year == last_year_of_10k_submission)
            
            # Queue the document extraction logic as an independent asynchronous task
            task = process_document(
                idx=idx,
                doc=doc,
                form_type=form_type,
                ticker=ticker,
                output_dir=output_dir_company_fillings_summarized,
                is_most_recent_of_its_type=is_most_recent_of_its_type,
                base_specs=agent_specs,
                file_manager=file_manager,
                extractor=extractor,
                heuristic_sec_cleaner=heuristic_sec_cleaner,
                llm_semaphore=llm_semaphore
            )
            extraction_tasks.append(task)

    print(f"\n🚀 Deploying {len(extraction_tasks)} document extraction tasks concurrently...")
    
    # Execute all queued tasks concurrently while letting the semaphore throttle the network
    await asyncio.gather(*extraction_tasks)
    
    print(f"\n🎯 HELIOS Pipeline execution complete for {ticker}.")


if __name__ == "__main__": 

    asyncio.run(run_extraction_pipeline(TICKER))
