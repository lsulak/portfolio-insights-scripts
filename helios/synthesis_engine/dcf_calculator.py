"""DCF Calculator — deterministic Damodaran-style 10-year DCF valuation.

Post-processes the StockValuationEngine markdown output:
    1. Locates the valuation report for the current quarter
    2. Extracts the embedded ``dcf_parameters`` JSON block
    3. Runs a two-stage DCF with terminal value
    4. Persists results as JSON

This module is purely deterministic — no AI model calls.
"""

import glob
import json
import logging
import os
import re
from dataclasses import asdict, dataclass

from helios.config import OutputDir
from helios.utils.commons import current_quarter

logger = logging.getLogger(__name__)

_COST_OF_CAPITAL = 0.15  # Strict HELIOS Hurdle Rate
_TERMINAL_ROIC = 0.15  # Terminal ROIC ≤ Cost of Capital (Damodaran constraint)
_STAGE_1_YEARS = 5  # High-growth period
_STAGE_2_YEARS = 5  # Fade-to-maturity period
_TOTAL_YEARS = _STAGE_1_YEARS + _STAGE_2_YEARS
_DEFAULT_SALES_TO_CAPITAL = 2.0  # Fallback to prevent division by zero


@dataclass(frozen=True)
class DCFParameters:
    """Input parameters extracted from the LLM valuation report."""

    base_year_revenue: float
    base_year_ebit: float
    effective_tax_rate: float
    stage_1_revenue_cagr: float
    target_operating_margin_year_5: float
    sales_to_capital_ratio: float
    total_debt: float
    cash_and_cash_equivalents_and_short_term_investments: float
    shares_outstanding: float
    market_cap: float
    stock_price: float
    risk_free_rate: float
    integrity_haircut_percent: float = 0.0


@dataclass(frozen=True)
class DCFResults:
    """Output of the deterministic DCF calculation."""

    calculated_firm_value: float
    calculated_equity_value: float
    intrinsic_value_per_share: float
    margin_of_safety_percent: float
    current_stock_price: float
    growth_dependency_ratio: float
    integrity_haircut_applied: bool


class DCFCalculator:

    def __init__(self, ticker_dir: str, force_recalculate: bool = False) -> None:
        self.ticker_dir = ticker_dir
        self.force_recalculate = force_recalculate

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> DCFResults:
        """Execute the full DCF pipeline: locate → extract → calculate → persist."""
        output_path = self._build_output_path()

        if not self.force_recalculate and os.path.exists(output_path):
            logger.info("[DCFCalculator] Output already exists at %s. Skipping.", output_path)
            with open(output_path, "r", encoding="utf-8") as f:
                return DCFResults(**json.load(f))

        valuation_file = self._find_valuation_file()
        params = self._extract_inputs(valuation_file)
        results = self._calculate(params)
        self._persist(output_path, results)
        return results

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        return os.path.join(
            self.ticker_dir, OutputDir.STOCK_VALUATION, f"dcf_results_{year}-Q{quarter}.json"
        )

    def _find_valuation_file(self) -> str:
        """Locate the single valuation markdown produced by StockValuationEngine."""
        pattern = os.path.join(self.ticker_dir, OutputDir.STOCK_VALUATION, "valuation_*.md")
        matches = glob.glob(pattern)

        if not matches:
            raise FileNotFoundError(
                f"No valuation markdown found matching {pattern}. "
                "Run StockValuationEngine first."
            )
        if len(matches) > 1:
            raise ValueError(
                f"Expected exactly one valuation file, found {len(matches)}: {matches}"
            )
        return matches[0]

    @staticmethod
    def _extract_inputs(filepath: str) -> DCFParameters:
        """Extract the embedded ``dcf_parameters`` JSON block from the valuation markdown."""
        with open(filepath, "r", encoding="utf-8") as f:
            markdown = f.read()

        match = re.search(r"```json\s*(\{.*?\})\s*```", markdown, re.DOTALL)
        if not match:
            raise ValueError(
                f"No JSON block found in {filepath}. "
                "The StockValuationEngine agent spec must produce an embedded ```json block."
            )

        raw = json.loads(match.group(1))
        dcf_params: dict = raw.get("dcf_parameters", raw)

        # Sanitize sales_to_capital_ratio to prevent division by zero
        s2c = dcf_params.get("sales_to_capital_ratio", _DEFAULT_SALES_TO_CAPITAL)
        dcf_params["sales_to_capital_ratio"] = s2c if s2c and s2c > 0 else _DEFAULT_SALES_TO_CAPITAL

        return DCFParameters(**dcf_params)

    @staticmethod
    def _calculate(params: DCFParameters) -> DCFResults:
        """Execute a deterministic 10-Year DCF based on Damodaran's First Principles.

        Two-stage model:
            Stage 1 (years 1–5):  high-growth at ``stage_1_revenue_cagr``,
                                  margin converges linearly to target.
            Stage 2 (years 6–10): growth fades linearly to risk-free rate,
                                  margin held at year-5 target.
            Terminal value:       perpetual growth at risk-free rate.
        """
        logger.info("[DCFCalculator] Starting calculation with: %s", params)

        base_margin = (
            params.base_year_ebit / params.base_year_revenue
            if params.base_year_revenue > 0
            else params.target_operating_margin_year_5
        )
        margin_step = (params.target_operating_margin_year_5 - base_margin) / _STAGE_1_YEARS

        logger.info("[DCFCalculator] Base margin: %.4f, margin step: %.4f", base_margin, margin_step)

        present_values: list[float] = []
        current_rev = params.base_year_revenue
        current_margin = base_margin
        fcff = 0.0

        # ── Stage 1: Years 1–5 (high-growth, sales-to-capital driven) ──
        for year in range(1, _STAGE_1_YEARS + 1):
            prev_rev = current_rev
            current_rev *= 1 + params.stage_1_revenue_cagr
            current_margin += margin_step

            nopat = current_rev * current_margin * (1 - params.effective_tax_rate)
            reinvestment = (current_rev - prev_rev) / params.sales_to_capital_ratio
            fcff = nopat - reinvestment

            pv = fcff / ((1 + _COST_OF_CAPITAL) ** year)
            present_values.append(pv)

        # ── Stage 2: Years 6–10 (macro-gravity fade to risk-free rate) ──
        growth_fade_step = (params.stage_1_revenue_cagr - params.risk_free_rate) / _STAGE_2_YEARS
        current_growth = params.stage_1_revenue_cagr

        for year in range(_STAGE_1_YEARS + 1, _TOTAL_YEARS + 1):
            current_growth -= growth_fade_step
            prev_rev = current_rev
            current_rev *= 1 + current_growth

            # Margin holds at the Year 5 structural target
            nopat = current_rev * params.target_operating_margin_year_5 * (1 - params.effective_tax_rate)

            if year == _TOTAL_YEARS:
                # Terminal fade: Reinvestment Rate = g / ROIC
                reinvestment = nopat * (params.risk_free_rate / _TERMINAL_ROIC)
            else:
                # Transition years maintain sales-to-capital mechanics
                reinvestment = (current_rev - prev_rev) / params.sales_to_capital_ratio

            fcff = nopat - reinvestment
            pv = fcff / ((1 + _COST_OF_CAPITAL) ** year)
            present_values.append(pv)

        # ── Terminal Value ──
        terminal_value = (fcff * (1 + params.risk_free_rate)) / (_COST_OF_CAPITAL - params.risk_free_rate)
        pv_terminal = terminal_value / ((1 + _COST_OF_CAPITAL) ** _TOTAL_YEARS)

        firm_value = sum(present_values) + pv_terminal
        equity_value = firm_value - params.total_debt + params.cash_and_equivalents

        logger.info(
            "[DCFCalculator] Firm value: %.2f, equity value (pre-haircut): %.2f",
            firm_value,
            equity_value,
        )

        # ── Integrity Haircut (asymmetrical risk modifier) ──
        equity_value_adjusted = equity_value * (1 - params.integrity_haircut_percent)

        intrinsic_per_share = (
            equity_value_adjusted / params.shares_outstanding
            if params.shares_outstanding > 0
            else 0.0
        )

        # ── Margin of Safety ──
        # Positive = undervalued (discount), Negative = overvalued (premium)
        margin_of_safety = 0.0
        if params.stock_price > 0 and intrinsic_per_share > 0:
            margin_of_safety = ((intrinsic_per_share - params.stock_price) / params.stock_price) * 100

        growth_dependency = pv_terminal / firm_value if firm_value > 0 else 0.0

        logger.info(
            "[DCFCalculator] Intrinsic/share: %.2f, MoS: %.2f%%, GDR: %.4f",
            intrinsic_per_share,
            margin_of_safety,
            growth_dependency,
        )

        return DCFResults(
            calculated_firm_value=round(firm_value, 2),
            calculated_equity_value=round(equity_value_adjusted, 2),
            intrinsic_value_per_share=round(intrinsic_per_share, 2),
            margin_of_safety_percent=round(margin_of_safety, 2),
            current_stock_price=params.stock_price,
            growth_dependency_ratio=round(growth_dependency, 4),
            integrity_haircut_applied=params.integrity_haircut_percent > 0,
        )

    @staticmethod
    def _persist(output_path: str, results: DCFResults) -> None:
        """Write DCF results to JSON."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(asdict(results), f, indent=4)
        logger.info("[DCFCalculator] Results saved to '%s'", output_path)
