"""Deep Research analyser — base class for agents with internet access.

These agents use the Gemini Deep Research (Interactions) API to autonomously
search the internet, gather information, and synthesise a report.
"""

import asyncio
import logging
from abc import abstractmethod
from pathlib import Path

from helios.config import EXTRACTION_AGENT_SPECS_DIR
from helios.utils.base_analyser import BaseAnalyser

logger = logging.getLogger(__name__)


class DeepResearchAnalyser(BaseAnalyser):
    """Template-method base for Gemini Deep Research Agent analysers.

    All Deep Research analysers (earnings calls, market, sector, business
    overview, external reality check) follow the same workflow:
        build output path → check cache → render agent spec → poll until
        done → write result.

    Subclasses only provide the pieces that vary:
        _get_model()          – which Gemini model to use
        _build_output_path()  – where to write the result
        _build_agent_spec()   – the rendered prompt

    Subclasses may override:
        AGENT_SPECS_DIR       – directory containing agent spec files
        _build_context()      – upstream context appended to the prompt
    """

    AGENT_SPECS_DIR: Path = EXTRACTION_AGENT_SPECS_DIR

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    @abstractmethod
    def _build_agent_spec(self) -> str:
        """Return the fully-rendered agent spec prompt."""

    def _build_context(self) -> str:
        """Return optional upstream context to append to the agent spec input.

        Override in subclasses that need to feed prior pipeline outputs
        (e.g. Business Overview, Narrative Validation) into the Deep Research
        agent alongside the prompt.
        """
        return ""

    # ------------------------------------------------------------------
    # Deep Research polling
    # ------------------------------------------------------------------

    async def _poll(self, agent_spec: str, label: str, polling_interval: int = 60, max_wait_minutes: int = 45) -> str:
        """Execute a Gemini Deep Research agent and poll until completion.

        Args:
            agent_spec: The rendered agent specification prompt.
            label: Human-readable label for log messages.
            polling_interval: Seconds between status checks.
            max_wait_minutes: Maximum total wait time before raising TimeoutError.

        Returns:
            The final synthesised research text.

        Raises:
            RuntimeError: If the agent execution fails or is cancelled.
            TimeoutError: If the agent does not complete within *max_wait_minutes*.
        """
        model = self._get_model()
        logger.info("Deploying Gemini Deep Research Agent via Interactions API...")

        context = self._build_context()
        full_input = f"{agent_spec}\n\n{context}"

        interaction = self.client.interactions.create(agent=model, input=full_input, background=True)
        interaction_id = interaction.id
        logger.info(f"[Interaction ID: {interaction_id}] - Agent dispatched. Entering polling loop...")

        max_iterations = (max_wait_minutes * 60) // polling_interval
        for iteration in range(max_iterations):
            current_state = self.client.interactions.get(id=interaction_id)
            status = current_state.status

            if status == "completed":
                logger.info(f"[{label}] --- RESEARCH COMPLETED ---")
                logger.debug(f"[{label}] Final Agent Output:\n{current_state.outputs}\n")
                return current_state.outputs[-1].text

            elif status in ("failed", "cancelled"):
                raise RuntimeError(f"[{label}] Agent execution failed with status: {status}")

            else:
                elapsed = (iteration + 1) * polling_interval
                logger.info(
                    f"[{label}] Agent Status: [{status.upper()}] - still crawling and synthesizing. "
                    f"Waiting {polling_interval}s... (elapsed: {elapsed // 60}m {elapsed % 60}s)"
                )
                await asyncio.sleep(polling_interval)

        raise TimeoutError(
            f"[{label}] Deep Research agent did not complete within {max_wait_minutes} minutes "
            f"(interaction_id: {interaction_id})"
        )

    # ------------------------------------------------------------------
    # Template method
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Execute the Deep Research Agent and persist the result."""
        output_path = self._build_output_path()
        if self._is_cached(output_path):
            return

        agent_spec = self._build_agent_spec()
        result = await self._poll(agent_spec, type(self).__name__)
        self._persist(output_path, result)
