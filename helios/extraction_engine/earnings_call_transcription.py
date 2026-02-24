import logging
import os
import time

from google import genai

from helios.config import AGENT_SPECS_DIR, GEMINI, SEC_EDGAR
from helios.utils.commons import deep_research_execution_sync, generate_agent_spec, load_agent_spec

AGENT_SPECS_FILEPATH = AGENT_SPECS_DIR / "earnings_calls_transcript_summary.md"

logger = logging.getLogger(__name__)


class EarningsCallTranscriptExtractor:
    """Orchestrates the extraction of earnings call transcripts using Gemini Deep Research Agent."""

    def __init__(self, client: genai.Client, output_base_dir: str, ticker: str, force_resummarize: bool = False):
        """Initialize the extractor with necessary parameters."""

        self.client = client
        self.output_base_dir = output_base_dir
        self.ticker = ticker
        self.force_resummarize = force_resummarize

    @staticmethod
    def construct_report_filename(output_base_dir: str, ticker: str, years_back: int) -> str:
        """Construct the output filename for the earnings call summary report."""
        curr_quarter = ((int(time.strftime("%m")) - 1) // 3) + 1
        curr_year = time.strftime("%Y")

        starting_year = int(time.strftime("%Y")) - years_back

        output_report_basedir = os.path.join(output_base_dir, ticker, "earnings_calls_summary")

        return os.path.join(
            output_report_basedir, f"from_{starting_year}Q{curr_quarter}_to_{curr_year}Q{curr_quarter}.md"
        )

    @staticmethod
    def construct_agent_spec(ticker: str, years_back: int) -> str:
        """Construct the keyword dictionary for agent spec substitution."""
        current_date = time.strftime("%Y-%m-%d")

        starting_year = int(time.strftime("%Y")) - years_back
        starting_date = f"{starting_year}{time.strftime('-%m-%d')}"

        agent_spec_keywords = {"TICKER": ticker, "FROM_DATE": starting_date, "TO_DATE": current_date}
        agent_spec_template = load_agent_spec(AGENT_SPECS_FILEPATH)
        agent_spec = generate_agent_spec(agent_spec_template, **agent_spec_keywords)

        return agent_spec

    def run(self) -> None:
        """Extract and summarize earnings call transcripts using Gemini Deep Research Agent."""
        # Prepare date variables for agent spec and filename
        years_back = SEC_EDGAR.years_back_earnings_calls

        output_filename = self.construct_report_filename(self.output_base_dir, self.ticker, years_back)

        # Check if report already exists and skip if not forcing resummarization
        if not self.force_resummarize and os.path.exists(output_filename):
            logger.info(f"Earnings call summary already exists at {output_filename}. Skipping extraction.")
            return

        agent_spec = self.construct_agent_spec(self.ticker, years_back)
        produced_insight = deep_research_execution_sync(
            self.client, model=GEMINI.earnings_call_model, agent_spec=agent_spec
        )

        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(produced_insight)

        logger.info(f"Earnings Calls summary written to file '{output_filename}'")
