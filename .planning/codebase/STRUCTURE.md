# Codebase Structure

**Analysis Date:** 2025-05-14

## Directory Layout

```
[project-root]/
├── data/               # Output data (reports, extracted filings, etc.)
│   ├── helios/         # Helios pipeline results per ticker
│   ├── input/          # Input directory for statement processing
│   └── output/         # Output directory for statement processing
├── docs/               # Documentation, diagrams, and sample outputs
├── helios/             # Core Helios AI pipeline implementation
│   ├── extraction_engine/ # Data gathering and minification
│   ├── synthesis_engine/  # Model building and narrative validation
│   ├── reasoning_engine/  # Final report and reality checking
│   ├── pipeline/       # Base classes and orchestration logic
│   └── utils/          # Shared utilities (API helpers, spec rendering)
├── logs/               # Execution logs
├── statement_processing/ # Financial statement parsing and loading
│   ├── queries/        # SQL templates for SQLite
│   └── [broker].py     # Specific broker implementation
├── helios_runner.py    # Main entry point for Helios
├── statement_runner.py # Main entry point for statement processing
├── Makefile            # Development and CI commands
├── requirements.txt    # Python dependencies
└── setup.sh            # Environment setup script
```

## Directory Purposes

**helios/:**
- Purpose: Contains the multi-agent AI pipeline for investment research.
- Contains: Engine subdirectories, base classes, and utility modules.
- Key files: `config.py` (Central configuration).

**statement_processing/:**
- Purpose: Handles ingestion of various broker transaction reports.
- Contains: Broker-specific modules and SQL queries.
- Key files: `database_utils.py` (SQLite operations).

**data/:**
- Purpose: Transient data storage for both pipelines.
- Contains: HTML/Markdown/JSON files for Helios; CSVs/SQLite for statements.
- Generated: Yes.
- Committed: No (usually ignored by `.gitignore`).

## Key File Locations

**Entry Points:**
- `helios_runner.py`: Orchestrates the fundamental analysis.
- `statement_runner.py`: Orchestrates the transaction ingestion.

**Configuration:**
- `helios/config.py`: Helios engine settings and paths.
- `.env`: API keys and SEC details.
- `statement_processing/constants.py`: Logging and other constants.

**Core Logic:**
- `helios/pipeline/base.py`: Execution model for analysers.
- `helios/utils/gemini_model_invoker.py`: Interface for Gemini API calls.

**Testing:**
- Not implemented (refer to `Makefile` for placeholder).

## Naming Conventions

**Files:**
- snake_case for Python files: `dcf_calculator.py`.
- kebab-case or underscore for shell scripts: `setup.sh`.

**Directories:**
- snake_case: `extraction_engine`.

## Where to Add New Code

**New Analysis Step (Helios):**
1. Implementation: Add to appropriate engine directory (`extraction_engine/`, etc.).
2. Specification: Add `.md` spec to `agent_specs/` within that engine.
3. Integration: Instantiate in `helios_runner.py`.

**New Broker (Statement Processing):**
1. Implementation: Add `[broker].py` to `statement_processing/`.
2. Registration: Add broker name to `__all__` in `statement_processing/__init__.py`.
3. SQL: Add `handle_[broker].sql` if custom SQL is needed.

**Utilities:**
- Shared helpers: `helios/utils/helpers.py`.

## Special Directories

**agent_specs/:**
- Purpose: Contains Markdown templates used as system prompts for AI agents.
- Committed: Yes.

**.VSCodeCounter/:**
- Purpose: Generated metrics for code size.
- Committed: No.

---

*Structure analysis: 2025-05-14*
