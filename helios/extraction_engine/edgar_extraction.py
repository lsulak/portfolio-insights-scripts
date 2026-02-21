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

from helios.utils.constants import EDGAR_EXTRACTOR_CREATIVITY_VARIANCE, TIMEOUT_BETWEEN_LLM_API_CALLS


# ==========================================
# DATA CONTRACTS
# ==========================================
@dataclass
class LocalSecDocument:
    ticker: str
    submission_year: int
    submission_order_for_the_year: int
    form_type: str
    local_file_path: str 
    mime_type: str        

@dataclass
class GeminiHostedFile:
    file_uri: str
    file_name: str 
    mime_type: str

# ==========================================
# INTERFACES
# ==========================================
class IEDGARFetcher(ABC):
    @abstractmethod
    async def fetch_latest_filings(self, ticker: str, form_type: str) -> Optional[List[LocalSecDocument]]:
        pass

class IGeminiFileManager(ABC):
    @abstractmethod
    async def upload_for_inference(self, document: LocalSecDocument) -> GeminiHostedFile:
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
class EDGARFetcher(IEDGARFetcher):
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

    async def fetch_latest_filings(self, ticker: str, form_type: str, years_back: int) -> Optional[List[LocalSecDocument]]:
        """
        Downloads the filing asynchronously to prevent blocking the main thread.
        Note: The SEC rate limits connections to 10 requests/second.
        """
        def file_to_ignore(filename):
            return filename.endswith('_final.txt') or filename.endswith('_primary.txt') or filename.endswith('_truncated.txt') or filename.endswith('_cleaned.txt')
        
        cutoff_date = self._get_report_cutoff_date(years_back)
        print(f"[SEC] Downloading {form_type} for {ticker} filed after {cutoff_date}...")
        
        await asyncio.to_thread(self.downloader.get, form_type, ticker, after=cutoff_date)

        # Locate the downloaded file. 
        search_pattern = os.path.join(
            self.download_dir, "helios", ticker, "sec_edgar_filings", form_type, "*", "*.txt"
        )
        downloaded_files = [
            fn for fn in glob.glob(search_pattern)
            if not file_to_ignore(os.path.basename(fn))
         ]

        if not downloaded_files:
            print(f"[SEC] No {form_type} found for {ticker}.")
            return None

        processed_docs = []
        for curr_file in downloaded_files:

            dir_of_curr_file = curr_file.split("/")[-2] # just the parent dir
            _, two_digits_submission_year, submission_order_for_the_year = dir_of_curr_file.split("-")
            four_digit_submission_year = datetime.strptime(str(two_digits_submission_year), "%y").year

            print(f"[SEC] Downloaded file for {ticker}, {form_type}, submission year: {four_digit_submission_year}, order: {submission_order_for_the_year}")

            curr_extraction = LocalSecDocument(
                ticker=ticker,
                submission_year=four_digit_submission_year,
                submission_order_for_the_year=submission_order_for_the_year,
                form_type=form_type,
                local_file_path=curr_file,
                mime_type="text/plain" # SEC primary submissions are SGML/Text
            )
            processed_docs.append(curr_extraction)
        
        return processed_docs
    

class GeminiFileManager(IGeminiFileManager):
    """Handles the lifecycle of large documents in the Gemini Files API."""

    def __init__(self, api_key: str):
        # The new SDK instantiates a Client rather than relying on global state
        self.client = genai.Client(api_key=api_key)

    async def upload_for_inference(self, document: LocalSecDocument) -> GeminiHostedFile:
        """
        Uploads the file to Google's servers. 
        Files uploaded here exist for 48 hours and cannot be downloaded back.
        """
        print(f"[GEMINI] Uploading {document.ticker} {document.form_type} to AI brain...")
        
        # Native async: no thread pool required.
        uploaded_file = await self.client.aio.files.upload(file=document.local_file_path)
        
        # Native pooling: we use asyncio.sleep to safely yield the event loop
        file_info = await self.client.aio.files.get(name=uploaded_file.name)
        
        while file_info.state.name == "PROCESSING":
            await asyncio.sleep(2)  
            file_info = await self.client.aio.files.get(name=uploaded_file.name)
            
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
        wait=wait_exponential(multiplier=4, min=4, max=120),
        stop=stop_after_attempt(TIMEOUT_BETWEEN_LLM_API_CALLS),
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
