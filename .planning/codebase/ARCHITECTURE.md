<!-- refreshed: 2025-05-14 -->
# Architecture

**Analysis Date:** 2025-05-14

## System Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                      Runners (Entry Points)                 │
│  `helios_runner.py`          `statement_runner.py`          │
└────────┬─────────────────────────────┬──────────────────────┘
         │                             │
         ▼                             ▼
┌──────────────────────┐      ┌───────────────────────────────┐
│    HELIOS Pipeline   │      │   Statement Processing        │
│    `helios/`         │      │   `statement_processing/`     │
├──────────────────────┤      ├───────────────────────────────┤
│  - Extraction Engine │      │  - Broker-specific Parsers    │
│  - Synthesis Engine  │      │  - SQL Query Templates        │
│  - Reasoning Engine  │      │  - Database Utils             │
└────────┬─────────────┘      └──────────────┬────────────────┘
         │                                   │
         ▼                                   ▼
┌──────────────────────┐      ┌───────────────────────────────┐
│   External APIs      │      │       Local Storage           │
│ (Gemini, SEC, YF)    │      │    (SQLite, Markdown, JSON)   │
└──────────────────────┘      └───────────────────────────────┘
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| Helios Runner | Orchestrates the multi-layered AI pipeline for company analysis. | `helios_runner.py` |
| Statement Runner | Orchestrates the ingestion of broker statements into SQLite. | `statement_runner.py` |
| Extraction Engine | Fetches and summarises raw data (SEC filings, earnings calls, market). | `helios/extraction_engine/` |
| Synthesis Engine | Validates narratives, compiles quantitative baselines, and calculates DCF. | `helios/synthesis_engine/` |
| Reasoning Engine | Performs reality checks and compiles the final investment report. | `helios/reasoning_engine/` |
| Pipeline Base | Provides base class and concurrency logic for analysers. | `helios/pipeline/base.py` |
| Broker Parsers | Reshape specific broker CSV exports (e.g., Revolut, IBKR) into a common schema. | `statement_processing/` |

## Pattern Overview

**Overall:** Pipeline / Layered Architecture

**Key Characteristics:**
- **Concurrent Execution:** Layers run analysers in parallel using `asyncio.gather`.
- **Stateless Analysis:** Each component is mostly stateless, relying on inputs and writing to filesystem.
- **Specification Driven:** AI agents are defined via Markdown templates (`agent_specs/`).

## Layers

**Extraction Layer:**
- Purpose: Gathers raw data from external sources and minifies/summarises it.
- Location: `helios/extraction_engine/`
- Contains: `EdgarExtractionPipeline`, `EarningsCallAnalyser`, `SectorAnalyser`, `MarketAnalyser`.
- Depends on: SEC EDGAR, Google Gemini, Yahoo Finance.
- Used by: Synthesis Layer.

**Synthesis Layer:**
- Purpose: Combines extracted data into meaningful financial models and narratives.
- Location: `helios/synthesis_engine/`
- Contains: `NarrativeValidator`, `QuantitativeBaselineCompiler`, `DCFCalculator`, `StockValuationEngine`.
- Depends on: Extraction Layer outputs.
- Used by: Reasoning Layer.

**Reasoning Layer:**
- Purpose: Final review and report compilation.
- Location: `helios/reasoning_engine/`
- Contains: `ExternalRealityChecker`, `FinalReportCompiler`.
- Depends on: Synthesis Layer outputs.

## Data Flow

### Helios Pipeline Flow

1. **Extraction:** Fetch filings and market data. Summarise for context. (`helios_runner.py:61`)
2. **Validation:** Cross-reference narratives with quantitative data. (`helios_runner.py:67`)
3. **Valuation:** Perform DCF calculations and qualitative synthesis. (`helios_runner.py:77`)
4. **Reasoning:** External reality check and final report generation. (`helios_runner.py:88`)

### Statement Processing Flow

1. **Ingestion:** Read CSV files from input directory. (`statement_runner.py:83`)
2. **Reshaping:** Map broker-specific columns to standard transaction schema. (`statement_processing/[broker].py`)
3. **Persistence:** Insert cleaned data into SQLite DB. (`statement_processing/database_utils.py`)

**State Management:**
- Helios state is managed via files in `data/helios/[TICKER]/`.
- Statement state is managed in a SQLite `.db` file.

## Key Abstractions

**BaseAnalyser:**
- Purpose: Common interface for all Helios components.
- Examples: `helios/pipeline/base.py`
- Pattern: Strategy / Template Method.

**AgentSpecRenderer:**
- Purpose: Renders Markdown templates for AI prompts using Jinja2.
- Examples: `helios/utils/agent_spec.py`

## Entry Points

**helios_runner.py:**
- Location: `./helios_runner.py`
- Triggers: CLI execution.
- Responsibilities: Pipeline orchestration, logging setup, argument parsing.

**statement_runner.py:**
- Location: `./statement_runner.py`
- Triggers: CLI execution.
- Responsibilities: Database creation, module import for specific brokers.

## Architectural Constraints

- **Threading:** Single-threaded event loop (`asyncio`) used for I/O bound API calls.
- **Global state:** Minimal. Configured via `helios/config.py`.
- **Circular imports:** Avoided by layered structure; common logic in `helios/utils/`.

## Anti-Patterns

### Sequential execution of independent tasks

**What happens:** Tasks that could run in parallel are run one by one.
**Why it's wrong:** Increases total execution time unnecessarily.
**Do this instead:** Use `BaseAnalyser.run_layer` which utilizes `asyncio.gather`.

## Error Handling

**Strategy:** Fail-fast for pipeline layers.

**Patterns:**
- `tenacity` retries for API calls.
- Try-except blocks around major entry points for clean exits.

## Cross-Cutting Concerns

**Logging:** Centralized configuration in `helios/utils/helpers.py`.
**Validation:** `pydantic` (indirectly via `google-genai`) and custom `NarrativeValidator`.
**Authentication:** API keys managed via `.env`.

---

*Architecture analysis: 2025-05-14*
