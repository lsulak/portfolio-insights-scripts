# Testing Patterns

**Analysis Date:** 2025-05-14

## Test Framework

**Runner:**
- `pytest` (referenced in `Makefile` and `requirements.txt`)
- Version: Not specified.
- Config: None found.

**Assertion Library:**
- Standard Python `assert` (presumed).

**Run Commands:**
```bash
make unit-tests         # Placeholder: Currently prints "Unit tests are not implemented"
```

## Test File Organization

**Location:**
- Not detected. No dedicated `tests/` directory or `test_*.py` files in the source tree.

**Naming:**
- Standard pattern `test_*.py` is expected by `pytest`.

## Test Structure

**Suite Organization:**
- Not implemented.

**Patterns:**
- No established patterns for setup, teardown, or assertions.

## Mocking

**Framework:**
- Not specified.

**What to Mock:**
- External APIs (Google Gemini, SEC, Yahoo Finance).
- File system operations.

## Fixtures and Factories

**Test Data:**
- Sample broker statements (CSVs) would be ideal.
- Sample SEC filings (HTML/Text).

**Location:**
- N/A.

## Coverage

**Requirements:**
- None enforced.

**View Coverage:**
- Placeholder in `Makefile`: `# python3 -m pytest -rxXs --cov`.

## Test Types

**Unit Tests:**
- Not implemented.

**Integration Tests:**
- Not implemented.

**E2E Tests:**
- Not implemented.

## Common Patterns

**Async Testing:**
- Since the core pipeline uses `asyncio`, future tests will likely require `pytest-asyncio`.

---

*Testing analysis: 2025-05-14*
