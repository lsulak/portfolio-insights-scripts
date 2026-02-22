import os
import glob
import asyncio
from datetime import datetime
from abc import ABC, abstractmethod
from typing import List, Optional
from dataclasses import dataclass

from sec_edgar_downloader import Downloader
from google import genai
from google.genai import types, errors
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from helios.utils.constants import (
    EDGAR_EXTRACTOR_CREATIVITY_VARIANCE,
    MAX_RETRY_ATTEMPTS,
    RETRY_MIN_WAIT_SECONDS,
    RETRY_MAX_WAIT_SECONDS
)


# ==========================================
# DATA CONTRACTS
# ==========================================
@dataclass
class LocalSECDocument:
    ticker: str
    submission_year: int
    submission_order_for_the_year: int
    form_type: str
    file_path_raw: str
    file_path_ai_ready: Optional[str]
    mime_type: str        

@dataclass
class GeminiHostedFile:
    file_uri: str
    file_name: str 
    mime_type: str

# ==========================================
# INTERFACES
# ==========================================
class ISECFetcher(ABC):
    @abstractmethod
    async def fetch_latest_filings(self, ticker: str, form_type: str) -> Optional[List[LocalSECDocument]]:
        pass

class IGeminiFileManager(ABC):
    @abstractmethod
    async def upload_for_inference(self, document: LocalSECDocument) -> GeminiHostedFile:
        pass

    @abstractmethod
    async def cleanup_remote_file(self, file_name: str) -> bool:
        pass

class IExtractorAgent(ABC):
    @abstractmethod
    async def generate_structured_dossier(self, ai_file: GeminiHostedFile, system_prompt: str) -> str:
        pass

# ==========================================
# CONCRETE IMPLEMENTATIONS
# ==========================================
class SECFetcher(ISECFetcher):
    """Handles interaction with the SEC EDGAR database."""
    
    def __init__(self, company_name: str, email_address: str, download_dir: str):
        # The SEC requires a User-Agent string formatted as "Company Name Email" to avoid blocking
        self.downloader = Downloader(company_name, email_address, download_dir)
        self.download_dir = download_dir

    def _get_report_cutoff_date(self, years_back: int) -> str:
        """Safely calculates the YYYY-MM-DD cutoff date, handling leap years."""
        today = datetime.now()
        try:
            cutoff = today.replace(year=today.year - years_back)
        except ValueError:
            cutoff = today.replace(year=today.year - years_back, month=2, day=28)
        return cutoff.strftime("%Y-%m-%d")

    async def fetch_latest_filings(self, ticker: str, form_type: str, years_back: int) -> Optional[List[LocalSECDocument]]:
        """
        Downloads the filing asynchronously to prevent blocking the main thread.
        Note: The SEC rate limits connections to 10 requests/second.
        """
        cutoff_date = self._get_report_cutoff_date(years_back)
        print(f"[SEC] Downloading Form '{form_type}' for {ticker} filed after {cutoff_date}...")
        
        await asyncio.to_thread(self.downloader.get, form_type, ticker, after=cutoff_date)

        # Locate the downloaded file. This is standard sub-location and cannot be changed.
        search_pattern = os.path.join(self.download_dir, "sec-edgar-filings", ticker, form_type, "*", "*.txt")
        downloaded_files = glob.glob(search_pattern)

        if not downloaded_files:
            print(f"[SEC] No {form_type} found for {ticker}.")
            return None

        processed_docs = []
        for curr_file in downloaded_files:

            dir_of_curr_file = curr_file.split("/")[-2] # just the parent dir
            _, two_digits_submission_year, submission_order_for_the_year = dir_of_curr_file.split("-")
            
            # Parse 2-digit year correctly (handles years after 2026)
            parsed_year = datetime.strptime(str(two_digits_submission_year), "%y").year
            current_year = datetime.now().year
            # If parsed year is more than 50 years before current, it's likely a future year
            if parsed_year < (current_year - 50):
                four_digit_submission_year = parsed_year + 100
            else:
                four_digit_submission_year = parsed_year

            print(f"[SEC] Downloaded file for {ticker}, {form_type}, submission year: {four_digit_submission_year}, order: {submission_order_for_the_year}")

            curr_extraction = LocalSECDocument(
                ticker=ticker,
                submission_year=four_digit_submission_year,
                submission_order_for_the_year=submission_order_for_the_year,
                form_type=form_type,
                file_path_raw=curr_file,
                file_path_ai_ready=None,
                mime_type="text/plain" # SEC primary submissions are SGML/Text
            )
            processed_docs.append(curr_extraction)
        
        return processed_docs
    

class GeminiFileManager(IGeminiFileManager):
    """Handles the lifecycle of large documents in the Gemini Files API."""

    def __init__(self, api_key: str):
        # The new SDK instantiates a Client rather than relying on global state
        self.client = genai.Client(api_key=api_key)

    async def upload_for_inference(self, document: LocalSECDocument) -> GeminiHostedFile:
        """
        Uploads the file to Google's servers. 
        Files uploaded here exist for 48 hours and cannot be downloaded back.
        """
        print(f"[GEMINI] Uploading {document.ticker} {document.form_type} to AI brain...")
        
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
                    
        print(f"[GEMINI] File ready: {uploaded_file.uri}")

        return GeminiHostedFile(
            file_uri=uploaded_file.uri,
            file_name=uploaded_file.name,
            mime_type=document.mime_type
        )

    async def cleanup_remote_file(self, file_name: str) -> bool:
        """Manually purges the document from Google servers."""
        try:
            # Native async delete
            await self.client.aio.files.delete(name=file_name)
            print(f"[GEMINI] Remote file {file_name} deleted.")
            return True
        except Exception as e:
            print(f"[GEMINI] Failed to delete remote file: {e}")
            return False
        

class ExtractorAgent(IExtractorAgent):
    """Executes the extraction prompt against the Gemini model."""
    
    def __init__(self, api_key: str, model_name: str):
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name

    def list_available_models(self):
        print("[GEMINI] Available models:")
        for model in self.client.models.list():
            print(model)

    # The rate limit shield: Automatically catches errors and applies 
    # exponential backoff.
    @retry(
        retry=retry_if_exception_type(errors.APIError),
        wait=wait_exponential(multiplier=4, min=RETRY_MIN_WAIT_SECONDS, max=RETRY_MAX_WAIT_SECONDS),
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        reraise=True
    )
    async def generate_structured_dossier(self, ai_file: GeminiHostedFile, system_prompt: str) -> str:
        """
        Forces the model to act as a structured extractor.
        """
        print(f"[AGENT] Extracting financials from {ai_file.file_name}...")
        
        config = types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=EDGAR_EXTRACTOR_CREATIVITY_VARIANCE, 
            response_mime_type="application/json" 
        )
            
        # Grab the file reference asynchronously
        file_ref = await self.client.aio.files.get(name=ai_file.file_name)

        # Native async generation
        response = await self.client.aio.models.generate_content(
            model=self.model_name,
            contents=[file_ref, "Extract the required data according to the system instructions."],
            config=config
        )
        return response.text
