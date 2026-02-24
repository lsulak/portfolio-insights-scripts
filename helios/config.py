"""HELIOS Configuration — single source of truth for all pipeline settings.

Configuration hierarchy (highest priority wins):
  CLI args  → Runtime user choices (--ticker, --force-resummarize)
  .env      → All operator settings (secrets, models, parallelism, data depth)
  Python    → Engineering constants only (retry tuning, temperature, rate limits)

Usage:
    from helios.config import GEMINI, SEC_EDGAR, AGENT_SPECS_DIR

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
    extractor_model: str
    earnings_call_model: str
    market_analysis_model: str
    sector_analysis_model: str
    max_parallel_calls: int

    # --- Python: resilience (engineering constants) ---
    max_retries: int = 5
    retry_min_wait_seconds: int = 4
    retry_max_wait_seconds: int = 150

    # --- Python: generation parameters ---
    extractor_temperature: float = 0.0
    max_chars_per_document: int = 900_000


# ==========================================
# SEC EDGAR DATA SOURCE
# ==========================================
@dataclass(frozen=True)
class SecEdgarConfig:
    """SEC EDGAR identity and data-depth — all from .env."""

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
# PATH CONSTANTS
# ==========================================
AGENT_SPECS_DIR = Path(__file__).resolve().parent / "extraction_engine" / "agent_specs"


# ==========================================
# CLI DEFAULTS
# ==========================================
DEFAULT_TICKER = os.getenv("TESTING_TICKER", "GOOGL")


# ==========================================
# MODULE-LEVEL SINGLETONS (loaded at import time)
# ==========================================
GEMINI = GeminiConfig(
    api_key=_env("GEMINI_API_KEY"),
    extractor_model=_env("EXTRACTOR_MODEL"),
    earnings_call_model=_env("EARNINGS_CALL_ANALYZER_MODEL"),
    market_analysis_model=_env("MARKET_ANALYSIS_MODEL"),
    sector_analysis_model=_env("SECTOR_ANALYSIS_MODEL"),
    max_parallel_calls=int(_env("GEMINI_MAX_PARALLEL_CALLS")),
)

SEC_EDGAR = SecEdgarConfig(
    company_name=_env("MY_COMPANY_NAME"),
    email=_env("MY_EMAIL"),
    years_back_10k=int(_env("YEARS_BACK_10K")),
    years_back_10q=int(_env("YEARS_BACK_10Q")),
    years_back_8k=int(_env("YEARS_BACK_8K")),
    years_back_def14a=int(_env("YEARS_BACK_DEF14A")),
    years_back_form4=int(_env("YEARS_BACK_FORM4")),
    years_back_earnings_calls=int(_env("YEARS_BACK_EARNINGS_CALLS")),
)
