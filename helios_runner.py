#!/usr/bin/env python3
"""HELIOS Pipeline - Main entry point for the multi-layered AI agent orchestraor
for company and its stock analysis.
"""

import asyncio
import logging
from logging.handlers import RotatingFileHandler
import os
import sys

from dotenv import load_dotenv
from google import genai

from helios.extraction_engine.sec.extraction_workflow import EdgarExtractionPipeline
from helios.utils.constants import GEMINI_API_KEY

load_dotenv()  # load environment variables from .env file

from helios.extraction_engine.earnings_call_transcription import EarningsCallTranscriptExtractor
from helios.utils.cli_parser import parse_cli_args

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

    client = genai.Client(api_key=GEMINI_API_KEY)

    result = await EdgarExtractionPipeline(
        ticker=args.ticker,
        output_base_dir=data_dir,
        force_resummarize=args.force_resummarize,
    ).run()

    EarningsCallTranscriptExtractor(
        client=client,
        output_base_dir=data_dir,
        ticker=args.ticker,
        force_resummarize=args.force_resummarize,
    ).run()

    return 0 if result.all_passed else 1


if __name__ == "__main__":
    setup_logging()

    args = parse_cli_args()
    exit_code = asyncio.run(main(args))

    sys.exit(exit_code)
