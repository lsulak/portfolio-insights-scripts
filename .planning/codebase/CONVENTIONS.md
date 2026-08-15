# Coding Conventions

**Analysis Date:** 2025-05-14

## Naming Patterns

**Files:**
- snake_case for all Python modules: `business_overview_synthesizer.py`.

**Functions:**
- snake_case for functions and methods: `run_layer()`, `process()`.

**Variables:**
- snake_case for variables: `layer_name`, `parallel_analysers`.

**Types:**
- PascalCase for classes: `BaseAnalyser`, `GeminiFileManager`.

## Code Style

**Formatting:**
- Tool: `black`.
- Settings: Line length 119 (from `Makefile`).

**Linting:**
- Tool: `flake8` and `pylint`.
- Key rules: Max line length 99 for linting checks, specific ignores for imports and docstrings (see `.flake8` and `.pylintrc`).

## Import Organization

**Order:**
1. Standard library imports (e.g., `os`, `sys`, `asyncio`).
2. Third-party library imports (e.g., `pandas`, `google.genai`).
3. Local project imports (e.g., `from helios.config import ...`).

**Path Aliases:**
- None detected. Uses standard relative/absolute imports within packages.

## Error Handling

**Patterns:**
- Use of `tenacity` for retrying flaky API calls.
- Fail-fast approach in pipeline layers.
- Global try-except in runners for logging and graceful exit.

## Logging

**Framework:** Python `logging` module.

**Patterns:**
- Centralized setup in `helios/utils/helpers.py`.
- Log to both console and files in `logs/` directory.

## Comments

**When to Comment:**
- Docstrings at the module level (detected in `helios_runner.py` and `statement_runner.py`).
- Inline comments for complex logic or TODOs.

**JSDoc/TSDoc:**
- Not applicable (Python project). Standard Python docstrings used.

## Function Design

**Size:** Generally small to medium, focused on a single responsibility.

**Parameters:** Use of keyword arguments and clear parameter names.

**Return Values:** Explicit return types are used in many signatures (e.g., `-> dict`, `-> None`).

## Module Design

**Exports:**
- Explicit `__all__` in `statement_processing/__init__.py`.
- Module-level imports in `helios/` subpackages to simplify runner access.

**Barrel Files:**
- Used in `statement_processing/` to expose supported platforms.

---

*Convention analysis: 2025-05-14*
