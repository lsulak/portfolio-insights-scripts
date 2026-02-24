"""SEC Filing Fetcher - Downloads filings from the SEC EDGAR database."""

import asyncio
import glob
import logging
import os
from datetime import datetime
from typing import List, Optional

from sec_edgar_downloader import Downloader

from helios.extraction_engine.sec.api import EdgarFormType, IEdgarFetcher, LocalEdgarDocument

logger = logging.getLogger(__name__)


class EdgarFetcher(IEdgarFetcher):
    """Downloads SEC filings from the EDGAR database.

    The SEC requires a User-Agent string formatted as "Company Name Email"
    to avoid blocking. Rate limits: 10 requests/second max.
    """

    def __init__(self, company_name: str, email_address: str, download_dir: str):
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

    async def fetch_latest_filings(
        self, ticker: str, form_type: EdgarFormType, years_back: int
    ) -> Optional[List[LocalEdgarDocument]]:
        """Downloads filings asynchronously to prevent blocking the main thread.

        Note: The SEC rate limits connections to 10 requests/second.
        """
        cutoff_date = self._get_report_cutoff_date(years_back)
        logger.info(f"[SEC] Downloading Form '{form_type}' for {ticker} filed after {cutoff_date}...")

        await asyncio.to_thread(self.downloader.get, form_type, ticker, after=cutoff_date)

        # Standard sub-path used by sec-edgar-downloader (cannot be changed)
        search_pattern = os.path.join(self.download_dir, "sec-edgar-filings", ticker, form_type, "*", "*.txt")
        downloaded_files = glob.glob(search_pattern)

        if not downloaded_files:
            logger.warning(f"[SEC] No {form_type} found for {ticker}.")
            return None

        processed_docs = []
        for curr_file in downloaded_files:
            dir_of_curr_file = os.path.basename(os.path.dirname(curr_file))
            _, two_digits_submission_year, submission_order_for_the_year = dir_of_curr_file.split("-")

            # Parse 2-digit year correctly (handles years after 2068 rollover)
            parsed_year = datetime.strptime(str(two_digits_submission_year), "%y").year
            current_year = datetime.now().year
            if parsed_year < (current_year - 50):
                four_digit_submission_year = parsed_year + 100
            else:
                four_digit_submission_year = parsed_year

            logger.debug(
                f"[SEC] Downloaded file for {ticker}, {form_type}, "
                f"submission year: {four_digit_submission_year}, order: {submission_order_for_the_year}"
            )

            curr_extraction = LocalEdgarDocument(
                ticker=ticker,
                submission_year=four_digit_submission_year,
                submission_order_for_the_year=int(submission_order_for_the_year),
                form_type=form_type,
                file_path_raw=curr_file,
                file_path_ai_ready=None,
                mime_type="text/plain",  # SEC primary submissions are SGML/Text
            )
            processed_docs.append(curr_extraction)

        return processed_docs
