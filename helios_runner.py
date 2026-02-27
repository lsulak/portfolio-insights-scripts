#!/usr/bin/env python3
"""HELIOS Pipeline - Main entry point for the multi-layered AI agent orchestraor
for company and its stock analysis.

It's a multi-layered AI system that orchestrates various specialized "agents" to perform a comprehensive analysis of a company and its stock. Steps:
    1A. Extract & Parse (EE, ETE) [Input EE: EDGAR, Ouptut EE: JSON; Input ETE: Deep Research of Earnings Calls Transcripts on the Internet, Output ETE: Markdown]
    1B. Extract & Summarize & Synthetize (SE, ME) [Input: Deep Research on the Internet, Output: Markdown]
    2A. Compile the Math (QBC) [Input: 1A, 1B, Output: Yaml]
    2B. Audit the Narrative (NV) [Input: 1A, 1B, SE, Output: Markdown]
    3. Valuate the Business (VE) [Input: QBC, NV, SE, Output: Markdown]
    4. Explain the Business (BE) [Input: NV, QBC, VE, SE, all 8Ks from EE, ETE, last 10K with Business and Risk Factor sections only, Output: Markdown]
    5. The External Reality Check (ERC) [Input: Deep Research on the Internet, BE and NV, Output: Markdown]
    6. Final Company Analyser (CE) [Input: QBC, NV, ME, VE, BE, ERC, Output: Markdown]

So Deep Search with Internet only: ETE, SE, ME, ERC - so 4x (cost maybe $5 per run)
"""

import asyncio
import logging
from logging.handlers import RotatingFileHandler
import os
import sys

from dotenv import load_dotenv

load_dotenv()  # Must be called before helios imports that read env vars

from google import genai

from helios.config import GEMINI
from helios.extraction_engine.earnings_calls_analyser import EarningsCallAnalyser
from helios.extraction_engine.edgar_analyser import EdgarExtractionPipeline
from helios.extraction_engine.market_analyser import MarketAnalyser
from helios.extraction_engine.sector_analyser import SectorAnalyser
from helios.utils.cli_parser import parse_cli_args

logger = logging.getLogger(__name__)

CURR_SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def setup_logging() -> None:
    """Configure logging for the application."""
    log_dir = os.path.join(CURR_SCRIPT_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f"{__name__}.log")
    rotating_handler = RotatingFileHandler(log_file, maxBytes=1000 * 1024, backupCount=10)  # 1 MB

    console_handler = logging.StreamHandler(sys.stdout)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[rotating_handler, console_handler],
        force=True,
    )

    # Mute chatty third-party libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("google_genai.models").setLevel(logging.WARNING)  # Google GenAI SDK's internal logging


async def main(args) -> int:
    """Main entry point for the HELIOS pipeline."""
    data_dir = os.path.join(CURR_SCRIPT_DIR, "data", "helios")
    os.makedirs(data_dir, exist_ok=True)

    client = genai.Client(api_key=GEMINI.api_key)

    analyser_names = (
        EdgarExtractionPipeline.__name__,
        EarningsCallAnalyser.__name__,
        SectorAnalyser.__name__,
        MarketAnalyser.__name__,
    )
    common_kwargs = dict(client=client, output_base_dir=data_dir, ticker=args.ticker)
    force_all = args.force_resummarize_all

    # All 4 extraction engines run concurrently — failures are isolated
    results = await asyncio.gather(
        EdgarExtractionPipeline(**common_kwargs, force_resummarize=force_all or args.force_resummarize_edgar).run(),
        EarningsCallAnalyser(**common_kwargs, force_resummarize=force_all or args.force_resummarize_earnings).run(),
        SectorAnalyser(**common_kwargs, force_resummarize=force_all or args.force_resummarize_sector).run(),
        MarketAnalyser(**common_kwargs, force_resummarize=force_all or args.force_resummarize_market).run(),
        return_exceptions=True,
    )

    # Report per-analyser outcomes
    any_failed = False
    for name, result in zip(analyser_names, results):
        if isinstance(result, BaseException):
            logger.error(f"❌ {name} failed: {type(result).__name__}: {result}")
            any_failed = True

    edgar_result = results[0]
    if isinstance(edgar_result, BaseException):
        return 1

    return 1 if any_failed or not edgar_result.all_passed else 0


if __name__ == "__main__":
    setup_logging()

    args = parse_cli_args()
    exit_code = asyncio.run(main(args))

    sys.exit(exit_code)
