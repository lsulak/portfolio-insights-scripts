"""CLI configuration for Helios."""

import argparse

from helios.utils.constants import TESTING_TICKER


def create_arg_parser() -> argparse.ArgumentParser:
    """Create argument parser for Helios pipeline.

    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        description="Extract and summarize all data relevant to stock investment analysis using AI.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--ticker",
        type=str,
        default=TESTING_TICKER,
        help=f"Stock ticker symbol to extract filings for (default: {TESTING_TICKER})",
    )

    parser.add_argument(
        "--force-resummarize",
        action="store_true",
        help="Force re-summarization of all documents even if summaries already exist",
    )

    return parser


def parse_cli_args():
    """Parse command line arguments for Helios pipeline.

    Returns:
        Parsed arguments namespace
    """
    parser = create_arg_parser()
    return parser.parse_args()
