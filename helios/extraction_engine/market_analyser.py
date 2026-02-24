import logging
import os
import time

from google import genai

from helios.config import AGENT_SPECS_DIR, GEMINI
from helios.utils.commons import deep_research_execution_sync, generate_agent_spec, load_agent_spec

AGENT_SPECS_FILEPATH = AGENT_SPECS_DIR / "market_analysis.md"

logger = logging.getLogger(__name__)


class MarketAnalyser:
    """Orchestrates the extraction of market analysis using Gemini Deep Research Agent."""

    def __init__(self, client: genai.Client, output_base_dir: str, ticker: str, force_resummarize: bool = False):
        """Initialize the extractor with necessary parameters."""

        self.client = client
        self.output_base_dir = output_base_dir
        self.ticker = ticker
        self.force_resummarize = force_resummarize

    @staticmethod
    def construct_report_filename(output_base_dir: str, ticker: str) -> str:
        """Construct the output filename for the market analysis report."""
        curr_quarter = ((int(time.strftime("%m")) - 1) // 3) + 1
        curr_year = time.strftime("%Y")

        output_report_basedir = os.path.join(output_base_dir, ticker, "market_analysis")

        return os.path.join(
            output_report_basedir, f"report_during_{curr_year}Q{curr_quarter}.md"
        )

    @staticmethod
    def construct_agent_spec(ticker: str) -> str:
        """Construct the keyword dictionary for agent spec substitution."""
        agent_spec_keywords = {"TICKER": ticker}
        agent_spec_template = load_agent_spec(AGENT_SPECS_FILEPATH)
        agent_spec = generate_agent_spec(agent_spec_template, **agent_spec_keywords)

        return agent_spec

    def run(self) -> None:
        """Extract and summarize market analysis using Gemini Deep Research Agent."""
        # Prepare date variables for agent spec and filename
        output_filename = self.construct_report_filename(self.output_base_dir, self.ticker)

        # Check if report already exists and skip if not forcing resummarization
        if not self.force_resummarize and os.path.exists(output_filename):
            logger.info(f"Market analysis report already exists at {output_filename}. Skipping extraction.")
            return

        agent_spec = self.construct_agent_spec(self.ticker)
        produced_insight = deep_research_execution_sync(
            self.client, model=GEMINI.market_analysis_model, agent_spec=agent_spec
        )

        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(produced_insight)

        logger.info(f"Market Analysis summary written to file '{output_filename}'")
