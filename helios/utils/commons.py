import logging
from string import Template
import time

logger = logging.getLogger(__name__)


def load_agent_spec(spec_path: str) -> Template:
    """Load a Markdown agent spec and compile it into a Jinja2 template.

    The Markdown file is the prompt — no intermediate parsing needed.
    Jinja2 variables (e.g. {{ business_and_risk }}) are resolved at render time.
    """
    logger.debug(f"Loading agent spec from {spec_path}...")

    with open(spec_path, "r", encoding="utf-8") as f:
        return Template(f.read())


def generate_agent_spec(spec_template: Template, **substitution_keywords) -> str:
    return spec_template.substitute(**substitution_keywords)


def deep_research_execution_sync(client, model: str, agent_spec: str, polling_interval: int = 60) -> str:
    logger.info(f"Deploying Gemini Deep Research Agent via Interactions API...")

    # We use the Interactions API to trigger the built-in Deep Research agent
    interaction = client.interactions.create(
        agent=model, input=agent_spec, background=True  # offload scraping loop to Google's servers
    )
    interaction_id = interaction.id
    logger.info(f"[Interaction ID: {interaction_id}] - Agent dispatched. Entering polling loop...\n")

    while True:
        # Fetch the current state from the server
        current_state = client.interactions.get(id=interaction_id)
        status = current_state.status

        if status == "completed":
            logger.info("\n--- RESEARCH COMPLETED ---\n")
            logger.debug(f"Final Agent Output:\n{current_state.outputs}\n")  # TODO REMOVE

            # The final synthesis is stored in the last output
            return current_state.outputs[-1].text

        elif status in ["failed", "cancelled"]:
            raise RuntimeError(f"Agent execution failed with status: {status}")

        else:
            logger.info(
                f"Agent Status: [{status.upper()}] - still crawling and synthesizing. Waiting {polling_interval}s..."
            )
            time.sleep(polling_interval)
