"""SEC-specific configuration - Filing types, extraction settings, and agent spec loading."""

import os
from pathlib import Path

from jinja2 import Template

from helios.extraction_engine.sec.api import EdgarFormType

# ==========================================
# SEC EXTRACTION SETTINGS
# ==========================================
SEC_EXTRACTOR_TEMPERATURE = 0.0

# SEC allows only 10 requests per second
# https://www.sec.gov/about/webmaster-frequently-asked-questions#code-support
SEC_API_CALL_DELAY = 5

# Context window size × 4 + buffer for prompt and response
MAX_CHARS_PER_DOCUMENT = 900_000

# ==========================================
# AGENT SPEC LOADING (Markdown + Jinja2)
# ==========================================
_AGENT_SPECS_DIR = Path(__file__).resolve().parent.parent / "agent_specs"

# Additional extraction context injected into the most recent 10-K only
AGENT_ADDITIONS_FIRST_10K_ONLY = "- Business: the core business model.\n- Risk Factors: the risk factors."

# Map each form type to its Markdown spec filename
_FORM_TO_SPEC_FILE = {
    EdgarFormType.ANNUAL_REPORT: "sec_10k.md",
    EdgarFormType.QUARTERLY_REPORT: "sec_10q.md",
    EdgarFormType.CURRENT_REPORT: "sec_8k.md",
    EdgarFormType.PROXY_STATEMENT: "sec_def14a.md",
    EdgarFormType.INSIDER_TRADING: "sec_form4.md",
}


def _load_agent_spec(filename: str) -> Template:
    """Load a Markdown agent spec and compile it into a Jinja2 template.

    The Markdown file is the prompt — no intermediate parsing needed.
    Jinja2 variables (e.g. {{ business_and_risk }}) are resolved at render time.
    """
    spec_path = _AGENT_SPECS_DIR / filename
    with open(spec_path, "r", encoding="utf-8") as f:
        return Template(f.read())


def _load_all_agent_specs() -> dict[EdgarFormType, Template]:
    """Load all SEC agent specs from Markdown files into Jinja2 templates."""
    return {form_type: _load_agent_spec(filename) for form_type, filename in _FORM_TO_SPEC_FILE.items()}


SEC_FORM_TO_AGENT_SPEC: dict[EdgarFormType, Template] = _load_all_agent_specs()

# ==========================================
# SEC FILING CONFIGURATION
# ==========================================
SEC_FORM_TO_YEARS_BACK: dict[EdgarFormType, int] = {
    EdgarFormType.ANNUAL_REPORT: int(os.getenv("YEARS_BACK_10K", "1")),
    EdgarFormType.QUARTERLY_REPORT: int(os.getenv("YEARS_BACK_10Q", "1")),
    EdgarFormType.CURRENT_REPORT: int(os.getenv("YEARS_BACK_8K", "1")),
    EdgarFormType.PROXY_STATEMENT: int(os.getenv("YEARS_BACK_DEF14A", "1")),
    EdgarFormType.INSIDER_TRADING: int(os.getenv("YEARS_BACK_FORM4", "1")),
}

SEC_FORMS_TO_CLEAN: tuple[EdgarFormType, ...] = (
    EdgarFormType.ANNUAL_REPORT,
    EdgarFormType.QUARTERLY_REPORT,
    EdgarFormType.PROXY_STATEMENT,
    EdgarFormType.CURRENT_REPORT,
)
