import os
from pathlib import Path

# Context window size × 4 + buffer for prompt and response
MAX_CHARS_PER_DOCUMENT = 900_000

# ==========================================
# AGENT SPEC LOADING (Markdown + Jinja2)
# ==========================================
AGENT_SPECS_DIR = Path(__file__).resolve().parent / "agent_specs"

YEARS_BACK_EARNINGS_CALLS = int(os.getenv("YEARS_BACK_EARNINGS_CALLS", "3"))
