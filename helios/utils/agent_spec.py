"""Agent spec template loading, rendering, and encapsulation via Jinja2."""

import logging
from pathlib import Path

from jinja2 import Template

logger = logging.getLogger(__name__)


class AgentSpecRenderer:
    """Loads and renders Jinja2 agent-spec templates from a directory.

    For one-off template loading without a directory context, use the
    static helper ``AgentSpecRenderer.load_template(path)``.
    """

    def __init__(self, specs_dir: Path) -> None:
        self._specs_dir = specs_dir

    def render(self, spec_filename: str, **keywords: str) -> str:
        """Load *spec_filename* from the specs directory and render it.

        Args:
            spec_filename: Name of the Markdown spec file.
            **keywords: Template variables to substitute.

        Returns:
            The fully rendered agent spec string.
        """
        template = self.load_template(self._specs_dir / spec_filename)
        return template.render(**keywords)

    @staticmethod
    def load_template(spec_path: Path | str) -> Template:
        """Load a Markdown agent spec and compile it into a Jinja2 template.

        Can be used independently of an ``AgentSpecRenderer`` instance when
        only the raw template is needed (e.g. for batch pre-loading).
        """
        logger.debug(f"Loading agent spec from {spec_path}...")
        with open(spec_path, "r", encoding="utf-8") as f:
            return Template(f.read())
