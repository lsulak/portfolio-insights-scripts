"""SEC-specific configuration - Filing types, extraction settings, and agent spec loading."""

import os
from string import Template

from helios.extraction_engine.commons import AGENT_SPECS_DIR
from helios.extraction_engine.sec.api import EdgarFormType
from helios.utils.commons import load_agent_spec

# ==========================================
# SEC EXTRACTION SETTINGS
# ==========================================
SEC_EXTRACTOR_TEMPERATURE = 0.0

# SEC allows only 10 requests per second
# https://www.sec.gov/about/webmaster-frequently-asked-questions#code-support
SEC_API_CALL_DELAY = 5


# Additional extraction context injected into the most recent 10-K only
AGENT_ADDITIONS_FIRST_10K_ONLY = """
    - Business: summary of extracted data from the business section, focused on the company's business description, including key products/services, markets, and competitive landscape. 
    - Risk Factors: summary of extracted data from the risk factors section.
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
    """Load all SEC agent specs from Markdown files into Jinja2 templates."""
    return {
        form_type: load_agent_spec(AGENT_SPECS_DIR / filename) for form_type, filename in _FORM_TO_SPEC_FILE.items()
    }


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
