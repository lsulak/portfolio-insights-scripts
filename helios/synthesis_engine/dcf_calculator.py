import json
import logging
import os
from typing import TypedDict, Dict, Any

from helios.config import OutputDir
from helios.utils.commons import current_quarter

logger = logging.getLogger(__name__)


class DCFParameters(TypedDict):
    base_year_revenue: float
    base_year_ebit: float
    tax_rate: float
    stage_1_revenue_cagr: float
    target_operating_margin_year_5: float
    sales_to_capital_ratio: float
    total_debt: float
    cash_and_equivalents: float
    shares_outstanding: float
    market_cap: float
    stock_price: float
    risk_free_rate: float
    integrity_haircut_percent: float

class DCFResults(TypedDict):
    calculated_firm_value: float
    calculated_equity_value: float
    intrinsic_value_per_share: float
    margin_of_safety_percent: float
    current_stock_price: float
    growth_dependency_ratio: float
    integrity_haircut_applied: bool

# Core Constants 
COST_OF_CAPITAL = 0.15  # Strict HELIOS Hurdle Rate
TERMINAL_ROIC = 0.15    # Damodaran constraint: Terminal ROIC cannot exceed Cost of Capital
    

def extract_dcf_inputs(filename: str) -> DCFParameters:
    """Extract embedded JSON string from the Markdown file."""
    # Assuming the JSON is enclosed in ```json ... ``` in the markdown
    with open(filename, "r") as f:
        markdown = f.read()

        match = re.search(r"```json\s*(\{.*?\})\s*```", markdown, re.DOTALL)
        if not match:
            raise ValueError("No JSON block found in VE markdown output.")
        regex_extracted_str = match.group(1)

        dcf_params = json.loads(regex_extracted_str)["dcf_parameters"]
        return DCFParameters(**dcf_params)


def calculate_damodaran_dcf(base_dir: str, params: DCFParameters) -> Dict[str, Any]:
    """
    Executes a deterministic 10-Year DCF based on Aswath Damodaran's First Principles.
    Translates the LLM parameter contract into an intrinsic value and margin of safety.
    """
    logger.info("Starting DCF calculation with parameters: %s", params)

    rev = params.base_year_revenue
    ebit = params.base_year_ebit
    tax = params.tax_rate
    cagr_1_5 = params.stage_1_revenue_cagr
    target_margin = params.target_operating_margin_year_5
    s2c = params.get("sales_to_capital_ratio", 2) # Fallback to 2 to prevent division by zero
    s2c = s2c if s2c > 0 else 2
    
    debt = params.total_debt
    cash = params.cash_and_equivalents
    shares = params.shares_outstanding
    rfr = params.risk_free_rate
    haircut = params.get("integrity_haircut_percent", 0.0)  # Fallback to no operation
    stock_price = params.stock_price

    # Base Year Margin logic (Protect against negative/zero base revenue)
    base_margin = (ebit / rev) if rev > 0 else target_margin
    margin_step = (target_margin - base_margin) / 5.0

    logger.info("DCF will use Base Margin: %.4f, Margin Step: %.4f", base_margin, margin_step)

    present_values = []
    current_rev = rev
    current_margin = base_margin
    
    # ==========================================
    # STAGE 1: Years 1 - 5 (Sales-to-Capital Driven)
    # ==========================================
    for year in range(1, 6):
        prev_rev = current_rev
        current_rev *= (1 + cagr_1_5)
        current_margin += margin_step
        
        current_ebit = current_rev * current_margin
        nopat = current_ebit * (1 - tax)
        
        # Reinvestment driven by revenue growth and capital intensity
        reinvestment = (current_rev - prev_rev) / s2c
        fcff = nopat - reinvestment
        
        pv = fcff / ((1 + COST_OF_CAPITAL) ** year)
        present_values.append(pv)

    # ==========================================
    # STAGE 2: Years 6 - 10 (Macro Gravity Fade)
    # ==========================================
    # Growth fades linearly to the Risk-Free Rate
    growth_fade_step = (cagr_1_5 - rfr) / 5.0
    current_growth = cagr_1_5
    
    for year in range(6, 11):
        current_growth -= growth_fade_step
        prev_rev = current_rev
        current_rev *= (1 + current_growth)
        
        # Margin holds at the Year 5 structural target
        current_ebit = current_rev * target_margin 
        nopat = current_ebit * (1 - tax)
        
        # Terminal fade for Reinvestment Rate: RR = g / ROIC
        if year == 10:
            terminal_reinvestment_rate = rfr / TERMINAL_ROIC
            reinvestment = nopat * terminal_reinvestment_rate
            fcff = nopat - reinvestment
        else:
            # Transition years maintain Sales-to-Capital mechanics
            reinvestment = (current_rev - prev_rev) / s2c
            fcff = nopat - reinvestment
            
        pv = fcff / ((1 + COST_OF_CAPITAL) ** year)
        present_values.append(pv)

    # ==========================================
    # TERMINAL VALUE & BRIDGING
    # ==========================================
    # Terminal Value using perpetual growth at Risk-Free Rate
    terminal_value = (fcff * (1 + rfr)) / (COST_OF_CAPITAL - rfr)
    pv_tv = terminal_value / ((1 + COST_OF_CAPITAL) ** 10)
    
    firm_value = sum(present_values) + pv_tv
    equity_value = firm_value - debt + cash
    logger.info("Calculated Firm Value: %.2f, Equity Value before haircut: %.2f", firm_value, equity_value)
    
    # The Asymmetrical Risk Modifier
    equity_value_adjusted = equity_value * (1 - haircut)
    
    intrinsic_value_per_share = (equity_value_adjusted / shares) if shares > 0 else 0.0
    logger.info("Calculated Intrinsic Value per Share: %.2f", intrinsic_value_per_share)

    # ==========================================
    # MARGIN OF SAFETY CALCULATION
    # ==========================================
    margin_of_safety_pct = 0.0
    if stock_price > 0 and intrinsic_value_per_share > 0:
        # Positive means undervalued (discount), Negative means overvalued (premium)
        margin_of_safety_pct = ((intrinsic_value_per_share - stock_price) / stock_price) * 100
    logger.info("Calculated Margin of Safety: %.2f%%", margin_of_safety_pct)

    growth_dependency_ratio = (pv_tv / firm_value) if firm_value > 0 else 0.0
    logger.info("Calculated Growth Dependency Ratio: %.4f", growth_dependency_ratio)

    dcf_results = DCFResults(
        calculated_firm_value=round(firm_value, 2),
        calculated_equity_value=round(equity_value_adjusted, 2),
        intrinsic_value_per_share=round(intrinsic_value_per_share, 2),
        margin_of_safety_percent=round(margin_of_safety_pct, 2),
        current_stock_price=stock_price,
        growth_dependency_ratio=round(growth_dependency_ratio, 4),
        integrity_haircut_applied=bool(haircut > 0)
    )
    
    year, quarter = current_quarter()
    filename = f"dcf_results_{year}-Q{quarter}.json"
    full_path = os.path.join(base_dir, OutputDir.STOCK_VALUATION, filename)
    
    with open (full_path, "w") as f:
        json.dump(dcf_results.__dict__, f, indent=4)
        logger.info("DCF results saved to '%s'", full_path)
