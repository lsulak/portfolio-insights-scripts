"""Managed agent analyser — base class for long-running agentic workflows.

These analysers delegate work to an AI agent that autonomously searches,
gathers information, and synthesises a report.  The caller polls until
the agent completes.

Currently backed by the Gemini Interactions (Deep Research) API, but the
abstraction allows future agent implementations (e.g. Google ADK custom
agents) without changing the contract for subclasses.
"""

import asyncio
import logging

from google import genai

from helios.pipeline.base import BaseAnalyser
from helios.utils.agent_spec import AgentSpecRenderer

logger = logging.getLogger(__name__)


class ManagedAgentAnalyser(BaseAnalyser):
    """Template-method base for analysers that delegate to a managed AI agent.

    All managed-agent analysers follow the same workflow:
        build output path → check cache → render agent spec → dispatch
        agent → poll until done → write result.

    Subclasses must set class attributes:
        AGENT_SPEC_FILE   – filename of the agent spec template

    Subclasses must implement:
        _get_model()          – which model / agent identifier to use
        _build_output_path()  – where to write the result

    Subclasses may override:
        _build_agent_spec()   – the rendered prompt (defaults to TICKER only)
        _build_context()      – upstream context appended to the prompt
    """

    AGENT_SPEC_FILE: str

    def __init__(
        self,
        client: genai.Client,
        spec_renderer: AgentSpecRenderer,
        output_base_dir: str,
        ticker: str,
        force_recompute: bool = False,
    ) -> None:
        super().__init__(output_base_dir, ticker, force_recompute)
        self.client = client
        self.spec_renderer = spec_renderer

    # ------------------------------------------------------------------
    # Agent spec
    # ------------------------------------------------------------------

    def _build_agent_spec(self) -> str:
        """Render the agent spec with the ticker.

        Override in subclasses that need additional template variables.
        """
        return self.spec_renderer.render(self.AGENT_SPEC_FILE, TICKER=self.ticker)

    # ------------------------------------------------------------------
    # Abstract / overridable
    # ------------------------------------------------------------------

    def _get_model(self) -> str:
        """Return the model / agent identifier for this analyser."""
        raise NotImplementedError

    def _build_context(self) -> str:
        """Return optional upstream context to append to the agent spec input.

        Override in subclasses that need to feed prior pipeline outputs
        into the agent alongside the prompt.
        """
        return ""

    # ------------------------------------------------------------------
    # Agent polling
    # ------------------------------------------------------------------

    async def _poll(self, agent_spec: str, label: str, polling_interval: int = 60, max_wait_minutes: int = 45) -> str:
        """Dispatch a managed AI agent and poll until completion.

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
        logger.info(f"Deploying managed AI agent via Interactions API for {label}...")

        context = self._build_context()
        full_input = f"{agent_spec}\n\n{context}"

        interaction = await asyncio.to_thread(
            self.client.interactions.create, agent=model, input=full_input, background=True
        )
        interaction_id = interaction.id
        logger.info(f"[Interaction ID: {interaction_id}] - Agent for {label} dispatched. Entering polling loop...")

        max_iterations = (max_wait_minutes * 60) // polling_interval
        for iteration in range(max_iterations):
            current_state = await asyncio.to_thread(self.client.interactions.get, id=interaction_id)
            status = current_state.status

            if status == "completed":
                logger.info(f"[{label}] --- RESEARCH COMPLETED ---")
                logger.debug(f"[{label}] Final Agent Output:\n{current_state.outputs}\n")
                if not current_state.outputs:
                    raise RuntimeError(f"[{label}] Agent completed but returned no outputs.")
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
            f"[{label}] Managed agent did not complete within {max_wait_minutes} minutes "
            f"(interaction_id: {interaction_id})"
        )

    # ------------------------------------------------------------------
    # Template method
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Dispatch the managed agent and persist the result."""
        output_path = self._build_output_path()
        if self._is_cached(output_path):
            return

        agent_spec = self._build_agent_spec()
        result = await self._poll(agent_spec, type(self).__name__)
        self._persist(output_path, result)
