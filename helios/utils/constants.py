import os


# ==========================================
# ENVIRONMENT CONFIGURATION
# ==========================================
def get_env_or_error(key: str) -> str:
    """Get environment variable or raise clear error."""
    value = os.getenv(key)
    if not value:
        raise ValueError(f"Missing required environment variable: {key}\n" f"Set it with: export {key}='your-value'")
    return value


GEMINI_API_KEY = get_env_or_error("GEMINI_API_KEY")
MY_COMPANY_NAME = get_env_or_error("MY_COMPANY_NAME")
MY_EMAIL = get_env_or_error("MY_EMAIL")

# ==========================================
# GENERAL CONFIGURATION
# ==========================================
TESTING_TICKER = get_env_or_error("TESTING_TICKER")

# Model selection
EXTRACTOR_MODEL = get_env_or_error("EXTRACTOR_MODEL")

# ==========================================
# GEMINI API RESILIENCE & CONCURRENCY
# ==========================================
GEMINI_MAX_RETRIES = 5
GEMINI_RETRY_MIN_WAIT_SECONDS = 4
GEMINI_RETRY_MAX_WAIT_SECONDS = 150
GEMINI_MAX_PARALLEL_CALLS = int(get_env_or_error("GEMINI_MAX_PARALLEL_CALLS"))
