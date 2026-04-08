#!/usr/bin/env python3
"""HELIOS Pipeline — multi-layered AI orchestrator that runs a fundamental investment analysis of a chosen publicly traded company."""

import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()  # Must be called before helios imports that read env vars

from google import genai

from helios.config import GEMINI, EXTRACTION_AGENT_SPECS_DIR, SYNTHESIS_AGENT_SPECS_DIR, REASONING_AGENT_SPECS_DIR
from helios.extraction_engine.earnings_calls_analyser import EarningsCallAnalyser
from helios.extraction_engine.edgar_analyser import EdgarExtractionPipeline
from helios.extraction_engine.market_analyser import MarketAnalyser
from helios.extraction_engine.sector_analyser import SectorAnalyser
from helios.synthesis_engine.dcf_calculator import DCFCalculator
from helios.synthesis_engine.narrative_validator import NarrativeValidator
from helios.synthesis_engine.quantitative_baseline_compiler import QuantitativeBaselineCompiler
from helios.synthesis_engine.stock_valuation_engine import StockValuationEngine
from helios.synthesis_engine.business_overview_synthesizer import BusinessOverviewSynthesizer
from helios.reasoning_engine.external_reality_checker import ExternalRealityChecker
from helios.reasoning_engine.final_report_compiler import FinalReportCompiler
from helios.pipeline.base import BaseAnalyser
from helios.utils.agent_spec import AgentSpecRenderer
from helios.utils.cli_parser import parse_cli_args
from helios.utils.gemini_file_manager import GeminiFileManager
from helios.utils.helpers import setup_logging

logger = logging.getLogger(__name__)

CURR_SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def bootstrap_pipeline(args) -> dict:
    """Instantiate all components and return the ordered pipeline layers."""
    data_dir = os.path.join(CURR_SCRIPT_DIR, "data", "helios")
    os.makedirs(data_dir, exist_ok=True)

    gemini_api_client = genai.Client(api_key=GEMINI.api_key)
    gemini_file_manager = GeminiFileManager(gemini_api_client)

    extraction_renderer = AgentSpecRenderer(EXTRACTION_AGENT_SPECS_DIR)
    synthesis_renderer = AgentSpecRenderer(SYNTHESIS_AGENT_SPECS_DIR)
    reasoning_renderer = AgentSpecRenderer(REASONING_AGENT_SPECS_DIR)

    p_common = dict(output_base_dir=data_dir, ticker=args.ticker, force_recompute=args.force_resummarize_all)

    return {
        "Extraction Engines": [
            EdgarExtractionPipeline(client=gemini_api_client, file_manager=gemini_file_manager, **p_common),
            EarningsCallAnalyser(client=gemini_api_client, spec_renderer=extraction_renderer, **p_common),
            SectorAnalyser(client=gemini_api_client, spec_renderer=extraction_renderer, **p_common),
            MarketAnalyser(client=gemini_api_client, spec_renderer=extraction_renderer, **p_common),
        ],
        "Narrative Validator & Quantitative Baseline": [
            NarrativeValidator(
                client=gemini_api_client,
                file_manager=gemini_file_manager,
                spec_renderer=synthesis_renderer,
                **p_common
            ),
            QuantitativeBaselineCompiler(
                client=gemini_api_client,
                file_manager=gemini_file_manager,
                spec_renderer=synthesis_renderer,
                **p_common
            ),
        ],
        "Stock Valuation & Business Overview": [
            StockValuationEngine(
                client=gemini_api_client,
                file_manager=gemini_file_manager,
                spec_renderer=synthesis_renderer,
                **p_common
            ),
            BusinessOverviewSynthesizer(client=gemini_api_client, spec_renderer=synthesis_renderer, **p_common),
        ],
        "Deterministic DCF": [
            DCFCalculator(**p_common),
        ],
        "External Reality Check": [
            ExternalRealityChecker(client=gemini_api_client, spec_renderer=reasoning_renderer, **p_common),
        ],
        "Final Report": [
            FinalReportCompiler(
                client=gemini_api_client,
                file_manager=gemini_file_manager,
                spec_renderer=reasoning_renderer,
                **p_common
            ),
        ],
    }


async def main(args) -> None:
    """Run the full HELIOS pipeline. It consists of multiple layers, each waits for the previous one, as they are dependent on their outputs.
    Each layer can have one or more analysers that run concurrently, as they are independent of each other.

    Any non-recoverable error within individual analysers will cause the pipeline to fail.
    This is because of the interdependencies of the outputs, and the fact that partial outputs are not useful.
    """
    pipeline = bootstrap_pipeline(args)

    for layer_name, parallel_analysers in pipeline.items():
        await BaseAnalyser.run_layer(layer_name, *parallel_analysers)

    logger.info("✅ HELIOS pipeline completed successfully.")


if __name__ == "__main__":
    setup_logging(os.path.join(CURR_SCRIPT_DIR, "logs"))
    args = parse_cli_args()
    try:
        asyncio.run(main(args))
    except Exception as e:
        logger.error("Pipeline failed: %s: %s", type(e).__name__, e)
        sys.exit(1)
