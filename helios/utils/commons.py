import logging
from pathlib import Path
import time

from jinja2 import Template

logger = logging.getLogger(__name__)


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


def deep_research_execution_sync(
    client, model: str, agent_spec: str, polling_interval: int = 60, max_wait_minutes: int = 30
) -> str:
    """Execute a Gemini Deep Research agent and poll until completion.

    Args:
        client: Gemini API client
        model: Model identifier for the deep research agent
        agent_spec: The rendered agent specification prompt
        polling_interval: Seconds between status checks
        max_wait_minutes: Maximum total wait time before raising TimeoutError

    Returns:
        The final synthesized research text

    Raises:
        RuntimeError: If the agent execution fails or is cancelled
        TimeoutError: If the agent does not complete within max_wait_minutes
    """
    logger.info("Deploying Gemini Deep Research Agent via Interactions API...")

    interaction = client.interactions.create(agent=model, input=agent_spec, background=True)
    interaction_id = interaction.id
    logger.info(f"[Interaction ID: {interaction_id}] - Agent dispatched. Entering polling loop...")

    max_iterations = (max_wait_minutes * 60) // polling_interval
    for iteration in range(max_iterations):
        current_state = client.interactions.get(id=interaction_id)
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
            time.sleep(polling_interval)

    raise TimeoutError(
        f"Deep Research agent did not complete within {max_wait_minutes} minutes "
        f"(interaction_id: {interaction_id})"
    )
