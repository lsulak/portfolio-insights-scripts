#!/usr/bin/env python3
"""HELIOS Pipeline - Main entry point for the multi-layered AI agent orchestraor
for company and its stock analysis.

It's a multi-layered AI system that orchestrates various specialized "agents" to perform a comprehensive analysis of a company and its stock. Steps:
    1A. Extract & Parse (EE, ETE) [Input EE: EDGAR, Ouptut EE: JSON; Input ETE: Deep Research of Earnings Calls Transcripts on the Internet, Output ETE: Markdown]
    1B. Extract & Summarize & Synthetize (SE, ME) [Input: Deep Research on the Internet, Output: Markdown]
    2A. Compile the Math (QBC) [Input: 1A, Output: Yaml]
    2B. Audit the Narrative (NV) [Input: 1A, SE, Output: Markdown]
    3A. Valuate the Business (VE) [Input: QBC, NV, SE, Output: Markdown]
    3B. Explain the Business (BE) [Input: NV, QBC, SE, all 8Ks from EE, ETE, last 10K with Business and Risk Factor sections only, Output: Markdown]
    4. The External Reality Check (ERC) [Input: Deep Research on the Internet, BE and NV, Output: Markdown]
    5. Final Company Analyser (CE) [Input: QBC, NV, ME, VE, BE, ERC, SE, Output: Markdown]

So Deep Search with Internet only: ETE, SE, ME, ERC - so 4x (cost maybe $6 per run) but never more than 3x in parallel.

TODOs
* Finalize code!
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
from helios.extraction_engine.earnings_calls_analyser import EarningsCallAnalyser
from helios.extraction_engine.edgar_analyser import EdgarExtractionPipeline
from helios.extraction_engine.market_analyser import MarketAnalyser
from helios.extraction_engine.sector_analyser import SectorAnalyser
from helios.synthesis_engine.narrative_validator import NarrativeValidator
from helios.synthesis_engine.quantitative_baseline_compiler import QuantitativeBaselineCompiler
from helios.synthesis_engine.stock_valuation_engine import StockValuationEngine
from helios.synthesis_engine.business_overview_synthetizer import BusinessOverviewSynthetizer
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

    Orchestration order:
        Layer 1: EE, ETE, SE, ME  (all 4 extraction engines in parallel)
        Layer 2: NV + QBC         (narrative validator & quantitative baseline in parallel)
        Layer 3: VE + BE          (stock valuation & business overview in parallel)
        Layer 4: ERC              (external reality check)
    """
    data_dir = os.path.join(CURR_SCRIPT_DIR, "data", "helios")
    os.makedirs(data_dir, exist_ok=True)

    client = genai.Client(api_key=GEMINI.api_key)
    common_kwargs = dict(client=client, output_base_dir=data_dir, ticker=args.ticker)
    force_all = args.force_resummarize_all

    # ── Layer 1: Extraction engines (all 4 concurrent) ───────────────
    logger.info("═══ Layer 1: Extraction Engines ═══")
    extraction_names = (
        EdgarExtractionPipeline.__name__,
        EarningsCallAnalyser.__name__,
        SectorAnalyser.__name__,
        MarketAnalyser.__name__,
    )
    extraction_results = await asyncio.gather(
        EdgarExtractionPipeline(**common_kwargs, force_resummarize=force_all or args.force_resummarize_edgar).run(),
        EarningsCallAnalyser(**common_kwargs, force_resummarize=force_all or args.force_resummarize_earnings).run(),
        SectorAnalyser(**common_kwargs, force_resummarize=force_all or args.force_resummarize_sector).run(),
        MarketAnalyser(**common_kwargs, force_resummarize=force_all or args.force_resummarize_market).run(),
        return_exceptions=True,
    )

    any_failed = _report_results(extraction_names, extraction_results)

    edgar_result = extraction_results[0]
    if isinstance(edgar_result, BaseException) or not edgar_result.all_passed:
        logger.error("Edgar extraction failed — cannot proceed to synthesis layers.")
        return 1
    if any_failed:
        logger.error("One or more extraction engines failed — cannot proceed to synthesis layers.")
        return 1

    # ── Layer 2: NV + QBC (parallel) ─────────────────────────────────
    logger.info("═══ Layer 2: Narrative Validator & Quantitative Baseline ═══")
    layer2_names = (NarrativeValidator.__name__, QuantitativeBaselineCompiler.__name__)
    layer2_results = await asyncio.gather(
        NarrativeValidator(**common_kwargs, force_resummarize=force_all).run(),
        QuantitativeBaselineCompiler(**common_kwargs, force_resummarize=force_all).run(),
        return_exceptions=True,
    )

    if _report_results(layer2_names, layer2_results):
        logger.error("Layer 2 failed — cannot proceed to valuation layer.")
        return 1

    # ── Layer 3: VE + BE (parallel) ──────────────────────────────────
    logger.info("═══ Layer 3: Stock Valuation & Business Overview ═══")
    layer3_names = (StockValuationEngine.__name__, BusinessOverviewSynthetizer.__name__)
    layer3_results = await asyncio.gather(
        StockValuationEngine(**common_kwargs, force_resummarize=force_all).run(),
        BusinessOverviewSynthetizer(**common_kwargs, force_resummarize=force_all).run(),
        return_exceptions=True,
    )

    if _report_results(layer3_names, layer3_results):
        logger.error("Layer 3 failed — cannot proceed to external reality check.")
        return 1

    # ── Layer 4: External Reality Check ──────────────────────────────
    logger.info("═══ Layer 4: External Reality Check ═══")
    try:
        await ExternalRealityChecker(**common_kwargs, force_resummarize=force_all).run()
    except Exception as e:
        logger.error(f"❌ ExternalRealityChecker failed: {type(e).__name__}: {e}")
        return 1

    # ── Layer 5: Final Report ────────────────────────────────────────
    logger.info("═══ Layer 5: Final Report ═══")
    try:
        await FinalReportCompiler(**common_kwargs, force_resummarize=force_all).run()
    except Exception as e:
        logger.error(f"❌ FinalReportCompiler failed: {type(e).__name__}: {e}")
        return 1

    logger.info("✅ HELIOS pipeline completed successfully.")
    return 0


def _report_results(names: tuple, results: tuple) -> bool:
    """Log per-analyser outcomes. Returns True if any failed."""
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
