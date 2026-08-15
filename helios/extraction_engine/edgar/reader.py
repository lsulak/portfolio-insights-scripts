"""Read summarized Edgar filings from the output directory."""

import os
from pathlib import Path

from helios.config import OutputDir
from helios.extraction_engine.edgar.domain import EdgarFormType


def collect_edgar_filings(ticker_dir: str, form_types=None, max_per_type=None):
    """Read summarized Edgar filings, optionally limiting per form type.

    Args:
        ticker_dir: Root ticker output directory (e.g. ``<base>/<TICKER>``).
        form_types: Iterable of ``EdgarFormType`` values to include.
                    Defaults to all form types.
        max_per_type: Dict mapping ``EdgarFormType`` to maximum number of
                      filings to include. ``None`` means unlimited.

    Returns:
        List of ``(label, content)`` tuples.
    """
    docs: list[tuple[str, str]] = []
    base = os.path.join(ticker_dir, OutputDir.EDGAR_SUMMARIZED)
    types_to_collect = form_types if form_types is not None else list(EdgarFormType)

    for form_type in types_to_collect:
        form_dir = os.path.join(base, form_type)
        if not os.path.isdir(form_dir):
            raise FileNotFoundError(f"Expected Edgar summarized directory not found: {form_dir}.")

        files = sorted(Path(form_dir).glob("*.json"), reverse=True)
        limit = (max_per_type or {}).get(form_type)
        if limit is not None:
            files = files[:limit]

        for fp in files:
            label = f"EDGAR {form_type}/{fp.name}"
            if limit == 1:
                label += " (latest only)"
            content = fp.read_text(encoding="utf-8")
            if len(content) == 0:
                raise ValueError(f"Empty content in {label}.")
            docs.append((label, content))

    return docs
