"""CLI configuration for Helios."""

import argparse

from helios.config import DEFAULT_TICKER


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
        default=DEFAULT_TICKER,
        help=f"Stock ticker symbol to extract filings for (default: {DEFAULT_TICKER})",
    )

    parser.add_argument(
        "--force-resummarize-all",
        action="store_true",
        help="Force re-summarization of ALL analysers, even if outputs already exist",
    )

    return parser


def parse_cli_args():
    """Parse command line arguments for Helios pipeline.

    Returns:
        Parsed arguments namespace
    """
    parser = create_arg_parser()
    return parser.parse_args()
