import asyncio
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from google import genai
from jinja2 import Template

from helios.config import AGENT_SPECS_DIR

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Agent spec helpers
# ------------------------------------------------------------------


def load_agent_spec(spec_path: Path | str) -> Template:
    """Load a Markdown agent spec and compile it into a Jinja2 template.

    The Markdown file is the prompt — no intermediate parsing needed.
    Jinja2 variables (e.g. {{ business_and_risk }}) are resolved at render time.
    """
    logger.debug(f"Loading agent spec from {spec_path}...")

    with open(spec_path, "r", encoding="utf-8") as f:
        return Template(f.read())


def generate_agent_spec(spec_template: Template, **substitution_keywords: str) -> str:
    """Render a Jinja2 agent spec template with the given variables."""
    return spec_template.render(**substitution_keywords)


# ------------------------------------------------------------------
# Deep Research base class
# ------------------------------------------------------------------


class DeepResearchAnalyser(ABC):
    """Template-method base for Gemini Deep Research Agent analysers.

    All Deep Research analysers (earnings calls, market, sector) follow the same
    workflow: build output path → check cache → render agent spec → poll until
    done → write result.  This base class captures that shared skeleton.

    Subclasses only provide the three pieces that vary:
        _get_model()          – which Gemini model to use
        _build_output_path()  – where to write the result
        _build_agent_spec()   – the rendered prompt
    """

    def __init__(
        self, client: genai.Client, output_base_dir: str, ticker: str, force_resummarize: bool = False
    ) -> None:
        self.client = client
        self.output_base_dir = output_base_dir
        self.ticker = ticker
        self.force_resummarize = force_resummarize

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    @abstractmethod
    def _get_model(self) -> str:
        """Return the Gemini model identifier for this analyser."""

    @abstractmethod
    def _build_output_path(self) -> str:
        """Return the full filesystem path for the output report."""

    @abstractmethod
    def _build_agent_spec(self) -> str:
        """Return the fully-rendered agent spec prompt."""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _current_quarter() -> tuple[str, int]:
        """Return ``(year_str, quarter_number)`` for the current date."""
        now = datetime.now()
        quarter = ((now.month - 1) // 3) + 1
        return str(now.year), quarter

    def _ticker_output_dir(self, subdir: str) -> str:
        """Return ``<output_base_dir>/<ticker>/<subdir>``."""
        return os.path.join(self.output_base_dir, self.ticker, subdir)

    @staticmethod
    def _render_agent_spec(spec_filename: str, **keywords: str) -> str:
        """Load an agent spec by filename and render it with the given keywords.

        Convenience wrapper combining ``load_agent_spec`` + ``generate_agent_spec``.
        Looks up the file under ``AGENT_SPECS_DIR``.
        """
        template = load_agent_spec(AGENT_SPECS_DIR / spec_filename)
        return generate_agent_spec(template, **keywords)

    # ------------------------------------------------------------------
    # Deep Research polling
    # ------------------------------------------------------------------

    async def _poll(self, agent_spec: str, polling_interval: int = 60, max_wait_minutes: int = 30) -> str:
        """Execute a Gemini Deep Research agent and poll until completion.

        Args:
            agent_spec: The rendered agent specification prompt
            polling_interval: Seconds between status checks
            max_wait_minutes: Maximum total wait time before raising TimeoutError

        Returns:
            The final synthesized research text

        Raises:
            RuntimeError: If the agent execution fails or is cancelled
            TimeoutError: If the agent does not complete within max_wait_minutes
        """
        model = self._get_model()
        logger.info("Deploying Gemini Deep Research Agent via Interactions API...")

        interaction = self.client.interactions.create(agent=model, input=agent_spec, background=True)
        interaction_id = interaction.id
        logger.info(f"[Interaction ID: {interaction_id}] - Agent dispatched. Entering polling loop...")

        max_iterations = (max_wait_minutes * 60) // polling_interval
        for iteration in range(max_iterations):
            current_state = self.client.interactions.get(id=interaction_id)
            status = current_state.status

            if status == "completed":
                logger.info("--- RESEARCH COMPLETED ---")
                logger.debug(f"Final Agent Output:\n{current_state.outputs}\n")
                return current_state.outputs[-1].text

            elif status in ("failed", "cancelled"):
                raise RuntimeError(f"Agent execution failed with status: {status}")

            else:
                elapsed = (iteration + 1) * polling_interval
                logger.info(
                    f"Agent Status: [{status.upper()}] - still crawling and synthesizing. "
                    f"Waiting {polling_interval}s... (elapsed: {elapsed // 60}m {elapsed % 60}s)"
                )
                await asyncio.sleep(polling_interval)

        raise TimeoutError(
            f"Deep Research agent did not complete within {max_wait_minutes} minutes "
            f"(interaction_id: {interaction_id})"
        )

    # ------------------------------------------------------------------
    # Template method
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Execute the Deep Research Agent and persist the result."""
        label = type(self).__name__
        output_path = self._build_output_path()

        if not self.force_resummarize and os.path.exists(output_path):
            logger.info(f"[{label}] Output already exists at {output_path}. Skipping.")
            return

        agent_spec = self._build_agent_spec()
        result = await self._poll(agent_spec)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result)

        logger.info(f"[{label}] Report written to '{output_path}'")
