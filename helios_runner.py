#!/usr/bin/env python3
"""HELIOS Pipeline - Main entry point for the multi-layered AI agent orchestrator
for company and its stock analysis.

It's a multi-layered AI system that orchestrates various specialized "agents" to perform a comprehensive analysis of a company and its stock. Steps:
    1A. Extract & Parse (EE, ETE) [Input EE: EDGAR, Output EE: JSON; Input ETE: Deep Research of Earnings Calls Transcripts on the Internet, Output ETE: Markdown]
    1B. Extract & Summarize & Synthesize (SE, ME) [Input: Deep Research on the Internet, Output: Markdown]
    2A. Compile the Math (QBC) [Input: 1A, Output: Yaml]
    2B. Audit the Narrative (NV) [Input: 1A, SE, Output: Markdown]
    3A. Valuate the Business (VE) [Input: QBC, NV, SE, Output: Markdown]
    3B. Explain the Business (BE) [Input: Deep Research on the Internet, NV, QBC, SE, ETE, last 10K with Business and Risk Factor sections only, Output: Markdown]
    4. The External Reality Check (ERC) [Input: Deep Research on the Internet, BE and NV, Output: Markdown]
    5. Final Company Analyser (CE) [Input: QBC, NV, ME, VE, BE, ERC, SE, Output: Markdown]

So Deep Search with Internet only: ETE, SE, ME, BE, ERC - so 5x (cost maybe $8 per run)
but never more than 3x Deep Research in parallel (using Paid Tier 1 in Google AI Studio).

TODOs
* Later: Upload some results to Google Drive? Maybe at least those that the final layer will work with? So that I can read it and rerun from Gemini Web UI
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
from helios.synthesis_engine.dcf_calculator import DCFCalculator
from helios.extraction_engine.earnings_calls_analyser import EarningsCallAnalyser
from helios.extraction_engine.edgar_analyser import EdgarExtractionPipeline
from helios.extraction_engine.market_analyser import MarketAnalyser
from helios.extraction_engine.sector_analyser import SectorAnalyser
from helios.synthesis_engine.narrative_validator import NarrativeValidator
from helios.synthesis_engine.quantitative_baseline_compiler import QuantitativeBaselineCompiler
from helios.synthesis_engine.stock_valuation_engine import StockValuationEngine
from helios.synthesis_engine.business_overview_synthesizer import BusinessOverviewSynthesizer
from helios.reasoning_engine.external_reality_checker import ExternalRealityChecker
from helios.reasoning_engine.final_report_compiler import FinalReportCompiler
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
    """Main entry point for the HELIOS pipeline.

    Orchestration order (each layer waits for the previous one):
        Layer 1: EE, ETE, SE, ME  (all 4 extraction engines in parallel)
        Layer 2: NV + QBC         (narrative validator & quantitative baseline)
        Layer 3: VE + BE          (stock valuation & business overview)
        Layer 4: ERC              (external reality check)
        Layer 5: CE               (final report)
    """
    data_dir = os.path.join(CURR_SCRIPT_DIR, "data", "helios")
    os.makedirs(data_dir, exist_ok=True)

    client = genai.Client(api_key=GEMINI.api_key)
    common = dict(client=client, output_base_dir=data_dir, ticker=args.ticker)

    # ── Layer 1: Extraction Engines ──────────────────────────────────
    if not await _run_extraction_layer(common, args.force_resummarize_all, args):
        return 1

    # ── Layer 2: Narrative Validator + Quantitative Baseline ─────────
    if not await _run_layer(
        "Narrative Validator & Quantitative Baseline",
        NarrativeValidator(**common, force_resummarize=args.force_resummarize_all),
        QuantitativeBaselineCompiler(**common, force_resummarize=args.force_resummarize_all),
    ):
        return 1

    # ── Layer 3: Stock Valuation + Business Overview ─────────────────
    if not await _run_layer(
        "Stock Valuation & Business Overview",
        StockValuationEngine(**common, force_resummarize=args.force_resummarize_all),
        BusinessOverviewSynthesizer(**common, force_resummarize=args.force_resummarize_all),
    ):
        return 1

    # ── Layer 3.5: Deterministic DCF (post-processes valuation output) ──
    try:
        DCFCalculator(
            ticker_dir=os.path.join(data_dir, args.ticker),
            force_recalculate=args.force_resummarize_all,
        ).run()
    except Exception as e:
        logger.error("DCF calculation failed — aborting pipeline: %s: %s", type(e).__name__, e)
        return 1

    # ── Layer 4: External Reality Check ──────────────────────────────
    if not await _run_layer(
        "External Reality Check", ExternalRealityChecker(**common, force_resummarize=args.force_resummarize_all)
    ):
        return 1

    # ── Layer 5: Final Report ────────────────────────────────────────
    if not await _run_layer(
        "Final Report", FinalReportCompiler(**common, force_resummarize=args.force_resummarize_all)
    ):
        return 1

    logger.info("✅ HELIOS pipeline completed successfully.")
    return 0


# ------------------------------------------------------------------
# Layer runners
# ------------------------------------------------------------------


async def _run_layer(label: str, *analysers) -> bool:
    """Run one or more analysers concurrently. Returns True if all succeeded."""
    logger.info(f"═══ {label} ═══")
    names = tuple(type(a).__name__ for a in analysers)

    results = await asyncio.gather(*(a.run() for a in analysers), return_exceptions=True)
    if _has_failures(names, results):
        logger.error(f"{label} failed — aborting pipeline.")
        return False
    return True


async def _run_extraction_layer(common: dict, force_all: bool, args) -> bool:
    """Layer 1: run all 4 extraction engines and validate Edgar results.

    Edgar is special — even if it finishes without an exception, we still
    check ``EdgarExtractionResult.all_passed`` to catch per-filing failures.
    """
    logger.info("═══ Extraction Engines ═══")
    analysers = (
        EdgarExtractionPipeline(**common, force_resummarize=force_all or args.force_resummarize_edgar),
        EarningsCallAnalyser(**common, force_resummarize=force_all or args.force_resummarize_earnings),
        SectorAnalyser(**common, force_resummarize=force_all or args.force_resummarize_sector),
        MarketAnalyser(**common, force_resummarize=force_all or args.force_resummarize_market),
    )
    names = tuple(type(a).__name__ for a in analysers)
    results = await asyncio.gather(*(a.run() for a in analysers), return_exceptions=True)

    any_failed = _has_failures(names, results)

    edgar_result = results[0]
    if isinstance(edgar_result, BaseException) or not edgar_result.all_passed:
        logger.error("Edgar extraction failed — cannot proceed to synthesis layers.")
        return False
    if any_failed:
        logger.error("One or more extraction engines failed — cannot proceed to synthesis layers.")
        return False
    return True


def _has_failures(names: tuple, results: tuple) -> bool:
    """Log per-analyser exceptions. Returns True if any failed."""
    any_failed = False
    for name, result in zip(names, results):
        if isinstance(result, BaseException):
            logger.error(f"❌ {name} failed: {type(result).__name__}: {result}")
            any_failed = True
    return any_failed


if __name__ == "__main__":
    setup_logging()

    args = parse_cli_args()
    exit_code = asyncio.run(main(args))

    sys.exit(exit_code)
