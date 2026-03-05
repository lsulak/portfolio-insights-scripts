"""Edgar domain knowledge — form types, data contracts, filing mappings, and agent spec loading.

This module is the single source of Edgar extraction domain knowledge.
All environment-driven configuration lives in helios.config.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from jinja2 import Template

from helios.config import EXTRACTION_AGENT_SPECS_DIR, EDGAR
from helios.utils.commons import load_agent_spec


# ==========================================
# EDGAR FORM TYPES
# ==========================================
class EdgarFormType(str, Enum):
    """Edgar filing form types supported by the extraction pipeline.

    Inherits from str so the enum value can be used directly as a string
    in file paths, dict keys, and log messages.
    """

    ANNUAL_REPORT = "10-K"
    QUARTERLY_REPORT = "10-Q"
    CURRENT_REPORT = "8-K"
    PROXY_STATEMENT = "DEF 14A"
    INSIDER_TRADING = "4"


# ==========================================
# DATA CONTRACTS
# ==========================================
@dataclass
class LocalEdgarDocument:
    """Represents a locally downloaded Edgar filing."""

    ticker: str
    submission_year: int
    submission_order_for_the_year: int
    form_type: EdgarFormType
    file_path_raw: str
    file_path_ai_ready: Optional[str]
    mime_type: str


# ==========================================
# AGENT SPEC LOADING
# ==========================================

# Additional extraction context injected into the most recent 10-K only
AGENT_ADDITIONS_FIRST_10K_ONLY = """
- Business: summary of extracted data from the business section, focused on the company's business description, including key products/services, markets, and competitive landscape. 
- Risk Factors: bulleted list of ALL explicitly named risk factors from the section Risk Factors in the filing, including a brief description of each found risk.
""".strip()

# Map each form type to its Markdown spec filename
_FORM_TO_SPEC_FILE = {
    EdgarFormType.ANNUAL_REPORT: "sec_10k.md",
    EdgarFormType.QUARTERLY_REPORT: "sec_10q.md",
    EdgarFormType.CURRENT_REPORT: "sec_8k.md",
    EdgarFormType.PROXY_STATEMENT: "sec_def14a.md",
    EdgarFormType.INSIDER_TRADING: "sec_form4.md",
}


def _load_all_agent_specs() -> dict[EdgarFormType, Template]:
    """Load all Edgar agent specs from Markdown files into Jinja2 templates."""
    return {
        form_type: load_agent_spec(EXTRACTION_AGENT_SPECS_DIR / filename) for form_type, filename in _FORM_TO_SPEC_FILE.items()
    }


EDGAR_FORM_TO_AGENT_SPEC: dict[EdgarFormType, Template] = _load_all_agent_specs()


# ==========================================
# EDGAR FILING BEHAVIOR
# ==========================================
EDGAR_FORM_TO_YEARS_BACK: dict[EdgarFormType, int] = {
    EdgarFormType.ANNUAL_REPORT: EDGAR.years_back_10k,
    EdgarFormType.QUARTERLY_REPORT: EDGAR.years_back_10q,
    EdgarFormType.CURRENT_REPORT: EDGAR.years_back_8k,
    EdgarFormType.PROXY_STATEMENT: EDGAR.years_back_def14a,
    EdgarFormType.INSIDER_TRADING: EDGAR.years_back_form4,
}

EDGAR_FORMS_TO_CLEAN: tuple[EdgarFormType, ...] = (
    EdgarFormType.ANNUAL_REPORT,
    EdgarFormType.QUARTERLY_REPORT,
    EdgarFormType.PROXY_STATEMENT,
    EdgarFormType.CURRENT_REPORT,
)
