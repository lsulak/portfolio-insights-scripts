"""DCF Calculator — deterministic Damodaran-style 10-year DCF valuation.

Post-processes the StockValuationEngine markdown output:
    1. Locates the valuation report for the current quarter
    2. Extracts the embedded ``dcf_parameters`` JSON block
    3. Runs a two-stage DCF with terminal value
    4. Persists results as JSON

This module is purely deterministic — no AI model calls.
"""

import json
import logging
import os
import re
from dataclasses import asdict, dataclass

from helios.config import OutputDir
from helios.pipeline.base import BaseAnalyser
from helios.utils.helpers import current_quarter

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


class DCFCalculator(BaseAnalyser):
    """Purely deterministic DCF calculator — no AI, no model calls.

    Extends ``BaseAnalyser`` for ticker-scoped directory helpers and caching.
    """

    # ------------------------------------------------------------------
    # BaseAnalyser contract
    # ------------------------------------------------------------------

    def _build_output_path(self) -> str:
        year, quarter = current_quarter()
        return os.path.join(self._ticker_dir(), OutputDir.STOCK_VALUATION, f"dcf_results_{year}-Q{quarter}.json")

    def run(self) -> None:
        """Execute the full DCF pipeline: locate → extract → calculate → persist."""
        output_path = self._build_output_path()

        if self._is_cached(output_path):
            return

        valuation_file = self._find_valuation_file()
        params = self._extract_inputs(valuation_file)
        results = self._calculate(params)
        self._persist_json(output_path, results)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _find_valuation_file(self) -> str:
        """Locate the valuation markdown for the current quarter."""
        year, quarter = current_quarter()
        expected = os.path.join(self._ticker_dir(), OutputDir.STOCK_VALUATION, f"valuation_{year}-Q{quarter}.md")

        if not os.path.exists(expected):
            raise FileNotFoundError(f"No valuation markdown found at {expected}. Run StockValuationEngine first.")
        return expected

    @staticmethod
    def _extract_inputs(filepath: str) -> DCFParameters:
        """Extract the embedded ``dcf_parameters`` JSON block from the valuation markdown."""
        with open(filepath, "r", encoding="utf-8") as f:
            markdown = f.read()

        # Find all fenced JSON blocks; pick the one containing "dcf_parameters"
        all_blocks = re.findall(r"```json\s*(\{.*?\})\s*```", markdown, re.DOTALL)
        if not all_blocks:
            raise ValueError(
                f"No JSON block found in {filepath}. "
                "The StockValuationEngine agent spec must produce an embedded ```json block."
            )

        dcf_block = next((b for b in all_blocks if "dcf_parameters" in b), all_blocks[-1])
        raw = json.loads(dcf_block)
        dcf_params: dict = raw.get("dcf_parameters", raw)

        # Sanitize sales_to_capital_ratio to prevent division by zero
        s2c = dcf_params.get("sales_to_capital_ratio", _DEFAULT_SALES_TO_CAPITAL)
        dcf_params["sales_to_capital_ratio"] = s2c if s2c and s2c > 0 else _DEFAULT_SALES_TO_CAPITAL

        # Clamp integrity_haircut_percent to [0, 1] — the agent spec emits a
        # fraction (e.g. 0.10) but the field name may mislead the LLM into
        # outputting a percentage (e.g. 10). Values > 1 are divided by 100.
        ihp = dcf_params.get("integrity_haircut_percent", 0.0)
        if ihp and ihp > 1:
            ihp = ihp / 100
        dcf_params["integrity_haircut_percent"] = max(0.0, min(ihp or 0.0, 1.0))

        # Normalise shares_outstanding to the same unit as market_cap (millions).
        # The LLM may emit absolute share count (e.g. 50_697_344) while
        # market_cap is in millions. Derive the expected value from
        # market_cap / stock_price and use it when the LLM value is off by > 10×.
        mcap = dcf_params.get("market_cap", 0) # in millions
        price = dcf_params.get("stock_price", 0)
        shares = dcf_params.get("shares_outstanding", 0) # can be in millions or absolute (mostly absolute)
        if mcap and price and shares:
            expected_shares = mcap / price
            ratio = shares / expected_shares
            if ratio > 10 or ratio < 0.1:
                logger.warning(
                    "shares_outstanding (%.2f) deviates %.1f× from market_cap/stock_price (%.4f). "
                    "Using derived value.",
                    shares,
                    ratio,
                    expected_shares,
                )
                dcf_params["shares_outstanding"] = expected_shares

        # Strip any extra keys the LLM might have added
        valid_fields = {f.name for f in DCFParameters.__dataclass_fields__.values()}
        dcf_params = {k: v for k, v in dcf_params.items() if k in valid_fields}

        return DCFParameters(**dcf_params)

    def _calculate(self, params: DCFParameters) -> DCFResults:
        """Execute a deterministic 10-Year DCF based on Damodaran's First Principles.

        Two-stage model:
            Stage 1 (years 1–5):  high-growth, margin converges to target.
            Stage 2 (years 6–10): growth fades to risk-free rate, margin held.
            Terminal value:       perpetual growth at risk-free rate.
        """
        logger.info("Starting DCF calculation with: %s", params)

        base_margin = (
            params.base_year_ebit / params.base_year_revenue
            if params.base_year_revenue > 0
            else params.target_operating_margin_year_5
        )
        logger.info("Base margin: %.4f", base_margin)

        pvs, rev, _ = self._stage_1(params, base_margin)
        stage2_pvs, rev, fcff = self._stage_2(params, rev)
        pvs.extend(stage2_pvs)

        pv_terminal = self._terminal_value(params, fcff)

        return self._assemble_results(params, pvs, pv_terminal)

    @staticmethod
    def _stage_1(params: DCFParameters, base_margin: float) -> tuple[list[float], float, float]:
        """Stage 1 (years 1–5): high-growth, sales-to-capital driven reinvestment."""
        margin_step = (params.target_operating_margin_year_5 - base_margin) / _STAGE_1_YEARS
        present_values: list[float] = []
        current_rev = params.base_year_revenue
        current_margin = base_margin
        fcff = 0.0

        for year in range(1, _STAGE_1_YEARS + 1):
            prev_rev = current_rev
            current_rev *= 1 + params.stage_1_revenue_cagr
            current_margin += margin_step

            nopat = current_rev * current_margin * (1 - params.effective_tax_rate)
            reinvestment = (current_rev - prev_rev) / params.sales_to_capital_ratio
            fcff = nopat - reinvestment
            present_values.append(fcff / ((1 + _COST_OF_CAPITAL) ** year))

        return present_values, current_rev, fcff

    @staticmethod
    def _stage_2(params: DCFParameters, current_rev: float) -> tuple[list[float], float, float]:
        """Stage 2 (years 6–10): growth fades linearly to risk-free rate."""
        growth_fade_step = (params.stage_1_revenue_cagr - params.risk_free_rate) / _STAGE_2_YEARS
        current_growth = params.stage_1_revenue_cagr
        present_values: list[float] = []
        fcff = 0.0

        for year in range(_STAGE_1_YEARS + 1, _TOTAL_YEARS + 1):
            current_growth -= growth_fade_step
            prev_rev = current_rev
            current_rev *= 1 + current_growth

            nopat = current_rev * params.target_operating_margin_year_5 * (1 - params.effective_tax_rate)

            if year == _TOTAL_YEARS:
                reinvestment = nopat * (params.risk_free_rate / _TERMINAL_ROIC)
            else:
                reinvestment = (current_rev - prev_rev) / params.sales_to_capital_ratio

            fcff = nopat - reinvestment
            present_values.append(fcff / ((1 + _COST_OF_CAPITAL) ** year))

        return present_values, current_rev, fcff

    @staticmethod
    def _terminal_value(params: DCFParameters, final_fcff: float) -> float:
        """Compute the present value of the terminal (perpetuity) value."""
        spread = _COST_OF_CAPITAL - params.risk_free_rate
        if spread <= 0:
            logger.warning(
                "Risk-free rate (%.4f) >= cost of capital (%.4f). "
                "Terminal value cannot be computed — setting to 0.",
                params.risk_free_rate,
                _COST_OF_CAPITAL,
            )
            terminal_value = 0.0
        else:
            terminal_value = (final_fcff * (1 + params.risk_free_rate)) / spread

        return terminal_value / ((1 + _COST_OF_CAPITAL) ** _TOTAL_YEARS)

    @staticmethod
    def _assemble_results(params: DCFParameters, present_values: list[float], pv_terminal: float) -> DCFResults:
        """Convert raw present values into the final DCFResults."""
        firm_value = sum(present_values) + pv_terminal
        equity_value = firm_value - params.total_debt + params.cash_and_cash_equivalents_and_short_term_investments
        # Apply governance haircut only when equity is positive — a penalty
        # should never make a negative equity value less negative.
        if equity_value > 0 and params.integrity_haircut_percent > 0:
            equity_value_adjusted = equity_value * (1 - params.integrity_haircut_percent)
        else:
            equity_value_adjusted = equity_value

        logger.info(
            "Firm value: %.2f, equity value (post-haircut): %.2f",
            firm_value,
            equity_value_adjusted,
        )

        intrinsic_per_share = (
            equity_value_adjusted / params.shares_outstanding if params.shares_outstanding > 0 else 0.0
        )

        margin_of_safety = 0.0
        if params.stock_price > 0 and intrinsic_per_share > 0:
            margin_of_safety = ((intrinsic_per_share - params.stock_price) / params.stock_price) * 100

        growth_dependency = pv_terminal / firm_value if firm_value > 0 else 0.0

        logger.info(
            "Intrinsic/share values for DCF: %.2f, MoS: %.2f%%, GDR: %.4f",
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
    def _persist_json(output_path: str, results: DCFResults) -> None:
        """Write DCF results to JSON."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(asdict(results), f, indent=4)
        logger.info("DCF results saved to '%s'", output_path)
