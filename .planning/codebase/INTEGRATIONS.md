# External Integrations

**Analysis Date:** 2025-05-14

## APIs & External Services

**AI Models:**
- Google Gemini API - Used for information extraction, synthesis, and reasoning.
  - SDK/Client: `google-genai`
  - Auth: `GEMINI_API_KEY` (env var)

**Financial Data:**
- SEC EDGAR - Fetching raw company filings (10-K, 10-Q, etc.).
  - SDK/Client: `sec-edgar-downloader`
  - Auth: User-agent details required by SEC.
- Yahoo Finance - Fetching market data.
  - SDK/Client: `yfinance`

## Data Storage

**Databases:**
- SQLite
  - Connection: Local file specified via CLI.
  - Client: `sqlite3` and `aiosql`.

**File Storage:**
- Local filesystem only.
  - Raw SEC filings stored in `data/helios/[TICKER]/company_filings_raw/`.
  - Intermediate and final reports stored in `data/helios/[TICKER]/`.

**Caching:**
- Filesystem-based caching for Helios analysers (check for existing files before re-running if `force_recompute` is False).

## Authentication & Identity

**Auth Provider:**
- API Key based (Google Gemini).

## Monitoring & Observability

**Error Tracking:**
- None (Local logging only).

**Logs:**
- Standard Python `logging`. Logs are written to the `logs/` directory.

## CI/CD & Deployment

**Hosting:**
- Local execution.

**CI Pipeline:**
- Makefile targets for linting (`lint`, `black-ci`, `flake8`, `pylint-shorter`).

## Environment Configuration

**Required env vars:**
- `GEMINI_API_KEY`: API key for Google Gemini.
- `SEC_EDGAR_COMPANY_NAME`: Company name for SEC downloader.
- `SEC_EDGAR_EMAIL`: Email address for SEC downloader.

**Secrets location:**
- `.env` file (not committed).

## Webhooks & Callbacks

**Incoming:**
- None

**Outgoing:**
- None

---

*Integration audit: 2025-05-14*
