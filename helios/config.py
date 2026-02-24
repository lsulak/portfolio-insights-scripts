"""HELIOS Configuration — single source of truth for all pipeline settings.

Configuration hierarchy (highest priority wins):
  CLI args  → Runtime user choices (--ticker, --force-resummarize)
  .env      → Secrets and operator preferences (API keys, models, parallelism, data depth)
  Python    → Engineering constants (retry tuning, temperature, rate limits, context window)

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
def _env(key: str, default: str | None = None) -> str:
    """Read an environment variable. Raises ValueError if required and missing."""
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(
            f"Missing required environment variable: {key}\n"
            f"Set it in your .env file or with: export {key}='your-value'"
        )
    return value


def _env_int(key: str, default: int) -> int:
    """Read an optional integer environment variable with a fallback default."""
    return int(os.getenv(key, str(default)))


# ==========================================
# GEMINI AI PLATFORM
# ==========================================
@dataclass(frozen=True)
class GeminiConfig:
    """All Gemini API settings — secrets, models, resilience, and generation parameters.

    .env values:
        GEMINI_API_KEY                 (required)
        EXTRACTOR_MODEL                (default: gemini-2.5-flash-lite)
        EARNINGS_CALL_ANALYZER_MODEL   (default: gemini-2.5-flash-lite)
        GEMINI_MAX_PARALLEL_CALLS      (default: 3)

    Python defaults (engineering constants — tuned once, rarely changed):
        max_retries, retry_min_wait_seconds, retry_max_wait_seconds,
        extractor_temperature, max_chars_per_document
    """

    # --- .env: secrets ---
    api_key: str = field(repr=False)

    # --- .env: model selection ---
    extractor_model: str = "gemini-2.5-flash-lite"
    earnings_call_model: str = "gemini-2.5-flash-lite"

    # --- .env: concurrency ---
    max_parallel_calls: int = 3

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
    """SEC EDGAR identity and data-depth preferences.

    .env values:
        MY_COMPANY_NAME              (required — SEC User-Agent compliance)
        MY_EMAIL                     (required — SEC User-Agent compliance)
        YEARS_BACK_10K               (default: 1)
        YEARS_BACK_10Q               (default: 1)
        YEARS_BACK_8K                (default: 1)
        YEARS_BACK_DEF14A            (default: 1)
        YEARS_BACK_FORM4             (default: 1)
        YEARS_BACK_EARNINGS_CALLS    (default: 1)

    Python defaults:
        api_call_delay_seconds       (5 — SEC rate limit: 10 req/s)
    """

    # --- .env: identity (required by SEC EDGAR) ---
    company_name: str
    email: str

    # --- .env: data depth (how far back to fetch each filing type) ---
    years_back_10k: int = 1
    years_back_10q: int = 1
    years_back_8k: int = 1
    years_back_def14a: int = 1
    years_back_form4: int = 1
    years_back_earnings_calls: int = 1

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
    extractor_model=_env("EXTRACTOR_MODEL", "gemini-2.5-flash-lite"),
    earnings_call_model=_env("EARNINGS_CALL_ANALYZER_MODEL", "gemini-2.5-flash-lite"),
    max_parallel_calls=_env_int("GEMINI_MAX_PARALLEL_CALLS", 3),
)

SEC_EDGAR = SecEdgarConfig(
    company_name=_env("MY_COMPANY_NAME"),
    email=_env("MY_EMAIL"),
    years_back_10k=_env_int("YEARS_BACK_10K", 1),
    years_back_10q=_env_int("YEARS_BACK_10Q", 1),
    years_back_8k=_env_int("YEARS_BACK_8K", 1),
    years_back_def14a=_env_int("YEARS_BACK_DEF14A", 1),
    years_back_form4=_env_int("YEARS_BACK_FORM4", 1),
    years_back_earnings_calls=_env_int("YEARS_BACK_EARNINGS_CALLS", 1),
)
