"""Edgar Filing Fetcher - Downloads filings from the Edgar database."""

import asyncio
import glob
import logging
import os
from datetime import datetime
from typing import List, Optional

from sec_edgar_downloader import Downloader

from helios.config import EDGAR
from helios.extraction_engine.edgar.domain import EdgarFormType, LocalEdgarDocument

logger = logging.getLogger(__name__)


class EdgarFetcher:
    """Downloads filings from the Edgar database.

    Edgar requires a User-Agent string formatted as "Company Name Email"
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

        Note: Edgar rate limits connections to 10 requests/second.
        """
        cutoff_date = self._get_report_cutoff_date(years_back)
        logger.info(f"[Edgar] Downloading Form '{form_type}' for {ticker} filed after {cutoff_date}...")

        await asyncio.to_thread(
            self.downloader.get, form_type, ticker, after=cutoff_date, limit=EDGAR.edgar_filings_per_type_limit
        )

        downloaded_files = self._discover_downloaded_files(ticker, form_type)
        if not downloaded_files:
            logger.warning(f"[Edgar] No {form_type} found for {ticker}.")
            return None

        return [self._parse_filing(path, ticker, form_type) for path in downloaded_files]

    def _discover_downloaded_files(self, ticker: str, form_type: EdgarFormType) -> list[str]:
        """Find all text files downloaded by sec-edgar-downloader for a given form type."""
        search_pattern = os.path.join(self.download_dir, "sec-edgar-filings", ticker, form_type, "*", "*.txt")
        return glob.glob(search_pattern)

    @staticmethod
    def _parse_filing(file_path: str, ticker: str, form_type: EdgarFormType) -> LocalEdgarDocument:
        """Parse a downloaded filing path into a ``LocalEdgarDocument``."""
        dir_name = os.path.basename(os.path.dirname(file_path))
        _, two_digit_year, submission_order = dir_name.split("-")

        submission_year = EdgarFetcher._resolve_four_digit_year(two_digit_year)

        logger.debug(
            f"[Edgar] Downloaded file for {ticker}, {form_type}, "
            f"submission year: {submission_year}, order: {submission_order}"
        )

        return LocalEdgarDocument(
            ticker=ticker,
            submission_year=submission_year,
            submission_order_for_the_year=int(submission_order),
            form_type=form_type,
            file_path_raw=file_path,
            file_path_ai_ready=None,
            mime_type="text/plain",
        )

    @staticmethod
    def _resolve_four_digit_year(two_digit_year: str) -> int:
        """Convert a 2-digit year string to 4 digits, handling rollover past 2068."""
        parsed_year = datetime.strptime(two_digit_year, "%y").year
        current_year = datetime.now().year
        if parsed_year < (current_year - 50):
            return parsed_year + 100
        return parsed_year
