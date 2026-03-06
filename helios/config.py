"""HELIOS Configuration — single source of truth for all pipeline settings.

Configuration hierarchy (highest priority wins):
  CLI args  → Runtime user choices (--ticker, --force-resummarize)
  .env      → All operator settings (secrets, models, parallelism, data depth)
  Python    → Engineering constants only (retry tuning, temperature, rate limits)

Usage:
    from helios.config import GEMINI, EDGAR, AGENT_SPECS_DIR

Important:
    load_dotenv() MUST be called before importing this module.
    This is enforced in helios_runner.py.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path


# ==========================================
# HELPERS
# ==========================================
def _env(key: str) -> str:
    """Read a required environment variable. Raises ValueError if missing."""
    value = os.getenv(key)
    if not value:
        raise ValueError(
            f"Missing required environment variable: {key}\n" f"Copy .env.example to .env and fill in your values."
        )
    return value


# ==========================================
# GEMINI AI PLATFORM
# ==========================================
@dataclass(frozen=True)
class GeminiConfig:
    """All Gemini API settings — from .env plus Python engineering constants."""

    # --- .env (required) ---
    api_key: str = field(repr=False)
    max_parallel_calls: int

    edgar_extractor_model: str
    earnings_call_analysis_model: str
    market_analysis_model: str
    sector_analysis_model: str
    external_reality_check_model: str
    narrative_validator_model: str
    quantitative_baseline_model: str
    stock_valuation_model: str
    business_overview_model: str
    final_report_model: str

    # --- Python: resilience (engineering constants) ---
    max_retries: int = 5
    retry_min_wait_seconds: int = 4
    retry_max_wait_seconds: int = 150

    # --- Python: generation parameters ---
    extractor_temperature: float = 0.0
    narrative_validator_temperature: float = 0.0
    quantitative_baseline_temperature: float = 0.0
    stock_valuation_temperature: float = 0.0
    business_overview_temperature: float = 0.0
    final_report_temperature: float = 0.0

    max_chars_per_document: int = 900_000


# ==========================================
# EDGAR DATA SOURCE
# ==========================================
@dataclass(frozen=True)
class EdgarConfig:
    """Edgar identity and data-depth — all from .env."""

    # --- .env (required) ---
    company_name: str
    email: str
    years_back_10k: int
    years_back_10q: int
    years_back_8k: int
    years_back_def14a: int
    years_back_form4: int
    years_back_earnings_calls: int

    # --- Python: rate limiting (engineering constant) ---
    api_call_delay_seconds: int = 5


# ==========================================
# OUTPUT DIRECTORY NAMES
# ==========================================
class OutputDir:
    """Per-ticker output subdirectory names — single source of truth.

    Every module that writes to or reads from ``<output_base_dir>/<ticker>/``
    must reference these constants instead of hardcoding strings.
    """

    SECTOR_ANALYSIS = "sector_analysis"
    MARKET_ANALYSIS = "market_analysis"

    EARNINGS_CALLS = "earnings_calls_synthesis"
    EDGAR_RAW = "company_filings_raw"
    EDGAR_MINIFIED = "company_filings_minified"
    EDGAR_SUMMARIZED = "company_filings_summarized"

    NARRATIVE_VALIDATION = "narrative_validation"
    QUANTITATIVE_BASELINE = "quantitative_baseline"

    STOCK_VALUATION = "stock_valuation"
    BUSINESS_OVERVIEW = "business_overview"
    EXTERNAL_REALITY_CHECK = "external_reality_check"
    FINAL_REPORT = "final_report"


# ==========================================
# PATH CONSTANTS
# ==========================================
EXTRACTION_AGENT_SPECS_DIR = Path(__file__).resolve().parent / "extraction_engine" / "agent_specs"
SYNTHESIS_AGENT_SPECS_DIR = Path(__file__).resolve().parent / "synthesis_engine" / "agent_specs"
REASONING_AGENT_SPECS_DIR = Path(__file__).resolve().parent / "reasoning_engine" / "agent_specs"


# ==========================================
# CLI DEFAULTS
# ==========================================
DEFAULT_TICKER = os.getenv("TESTING_TICKER", "GOOGL")


# ==========================================
# MODULE-LEVEL SINGLETONS (loaded at import time)
# ==========================================
GEMINI = GeminiConfig(
    api_key=_env("GEMINI_API_KEY"),
    max_parallel_calls=int(_env("GEMINI_MAX_PARALLEL_CALLS")),
    edgar_extractor_model=_env("EDGAR_EXTRACTOR_MODEL"),
    earnings_call_analysis_model=_env("EARNINGS_CALL_ANALYZER_MODEL"),
    market_analysis_model=_env("MARKET_ANALYSIS_MODEL"),
    sector_analysis_model=_env("SECTOR_ANALYSIS_MODEL"),
    external_reality_check_model=_env("EXTERNAL_REALITY_CHECK_MODEL"),
    narrative_validator_model=_env("NARRATIVE_VALIDATOR_MODEL"),
    quantitative_baseline_model=_env("QUANTITATIVE_BASELINE_MODEL"),
    stock_valuation_model=_env("STOCK_VALUATION_MODEL"),
    business_overview_model=_env("BUSINESS_OVERVIEW_MODEL"),
    final_report_model=_env("FINAL_REPORT_MODEL"),
)

EDGAR = EdgarConfig(
    company_name=_env("MY_COMPANY_NAME"),
    email=_env("MY_EMAIL"),
    years_back_10k=int(_env("YEARS_BACK_10K")),
    years_back_10q=int(_env("YEARS_BACK_10Q")),
    years_back_8k=int(_env("YEARS_BACK_8K")),
    years_back_def14a=int(_env("YEARS_BACK_DEF14A")),
    years_back_form4=int(_env("YEARS_BACK_FORM4")),
    years_back_earnings_calls=int(_env("YEARS_BACK_EARNINGS_CALLS")),
)
