# Technology Stack

**Analysis Date:** 2025-05-14

## Languages

**Primary:**
- Python 3.12 - Used for the entire Helios pipeline and statement processing logic.

**Secondary:**
- Bash - Used for setup scripts (`setup.sh`) and utility scripts (`combine_agent_specs.sh`).
- SQL - Used for database schema and data manipulation in `statement_processing/queries/`.

## Runtime

**Environment:**
- Python 3.12

**Package Manager:**
- pip
- Lockfile: Missing (uses `requirements.txt`)

## Frameworks

**Core:**
- `google-genai` - Interface with Google Gemini models.
- `pandas` - Data manipulation for financial statements.
- `aiosql` - Async SQL execution for SQLite.
- `sec-edgar-downloader` - Fetching SEC filings.
- `yfinance` - Market data retrieval.
- `jinja2` - Templating for agent specifications.

**Testing:**
- `pytest` - Referenced in `Makefile` but not currently implemented.

**Build/Dev:**
- `black` - Code formatting (line length 119).
- `flake8` - Linting (line length 99).
- `pylint` - Static code analysis.

## Key Dependencies

**Critical:**
- `google-genai` - Essential for the Helios AI pipeline.
- `pandas` - Essential for statement processing.
- `tenacity` - Used for robust API call retries.

**Infrastructure:**
- `python-dotenv` - Environment variable management.
- `sqlite3` - Local data storage for transactions.

## Configuration

**Environment:**
- Configured via `.env` file.
- Key configs: `GEMINI_API_KEY`, `SEC_EDGAR_COMPANY_NAME`, `SEC_EDGAR_EMAIL`.

**Build:**
- `Makefile` - Orchestrates linting and environment setup.
- `.flake8` - Linting configuration.
- `.pylintrc` - Pylint configuration.

## Platform Requirements

**Development:**
- Linux/macOS preferred (uses Bash scripts and Makefile).
- Python 3.12+.

**Production:**
- Local execution environment with access to Google Gemini API.

---

*Stack analysis: 2025-05-14*
