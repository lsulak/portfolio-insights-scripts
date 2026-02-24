"""SEC-specific domain constants — filing type mappings, agent spec loading, and prompt fragments.

This module contains SEC extraction *domain knowledge* only.
All environment-driven configuration lives in helios.config.
"""

from jinja2 import Template

from helios.config import AGENT_SPECS_DIR, SEC_EDGAR
from helios.extraction_engine.sec.api import EdgarFormType
from helios.utils.commons import load_agent_spec

# ==========================================
# AGENT SPEC LOADING
# ==========================================

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
# SEC FILING BEHAVIOR
# ==========================================
SEC_FORM_TO_YEARS_BACK: dict[EdgarFormType, int] = {
    EdgarFormType.ANNUAL_REPORT: SEC_EDGAR.years_back_10k,
    EdgarFormType.QUARTERLY_REPORT: SEC_EDGAR.years_back_10q,
    EdgarFormType.CURRENT_REPORT: SEC_EDGAR.years_back_8k,
    EdgarFormType.PROXY_STATEMENT: SEC_EDGAR.years_back_def14a,
    EdgarFormType.INSIDER_TRADING: SEC_EDGAR.years_back_form4,
}

SEC_FORMS_TO_CLEAN: tuple[EdgarFormType, ...] = (
    EdgarFormType.ANNUAL_REPORT,
    EdgarFormType.QUARTERLY_REPORT,
    EdgarFormType.PROXY_STATEMENT,
    EdgarFormType.CURRENT_REPORT,
)
