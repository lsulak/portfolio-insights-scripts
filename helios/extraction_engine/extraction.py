"""Extraction Engine Facade - High-level API for all extraction pipelines.

The runner should only interact with this module. Individual subsystem
details (SEC, earnings calls, market analysis, etc.) stay encapsulated
inside their respective packages.
"""

import logging
from dataclasses import dataclass
from typing import Dict

from helios.extraction_engine.sec import EdgarExtractionPipeline

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Outcome of an extraction pipeline run."""

    total: int
    successful: int
    failed: int

    @property
    def all_passed(self) -> bool:
        return self.failed == 0


async def run_edgar_extraction(ticker: str, output_base_dir: str, force_resummarize: bool = False) -> ExtractionResult:
    """Run the EDGAR filing extraction pipeline for a given ticker.

    Args:
        ticker: Stock ticker symbol (e.g. "GOOGL").
        output_base_dir: Root directory for output artifacts.
        force_resummarize: Re-process filings even if summaries already exist.

    Returns:
        ExtractionResult with counts of processed documents.
    """
    pipeline = EdgarExtractionPipeline(
        ticker=ticker,
        output_base_dir=output_base_dir,
        force_resummarize=force_resummarize,
    )
    stats: Dict[str, int] = await pipeline.run()

    return ExtractionResult(
        total=stats["total"],
        successful=stats["successful"],
        failed=stats["failed"],
    )
