# ADR 005: Skill-Based Decomposition (Capability vs. Persona)

**Date:** 2026-05-23
**Status:** Proposed

## Context
The current Helios system uses highly specific agent specifications (e.g., `sec_10k.md`, `earnings_calls_analysis.md`) that are coupled to a linear pipeline. The user wants to reuse the "lessons and ideas" from these specs while building a new, bi-directional, and agentic system from scratch, avoiding the bias of the old system's rigid workflow.

## Options Considered
1. **Direct Porting:** Rewriting the old specs as ADK Agents. (Preserves bias and linear thinking).
2. **Fresh Start:** Deleting all old specs and starting from zero. (Loses years of tuned extraction logic and domain expertise).
3. **Skill-Based Decomposition:** Decoupling the *Who* (Personas like Bull/Bear) from the *What* (Skills like SEC Auditing or Market Analysis).

## Decision
We will adopt a **Skill-Based Decomposition** architecture:

1. **The Skill Library:** The old agent specifications will be refactored into a "Library of Skills." For example, `sec_10k.md` becomes the `SEC_Forensic_Auditor_Skill`. These are **stateless, functional tools** that return structured data.
2. **The Personas:** The "Bull," "Bear," and "Moderator" agents are the **stateful actors**. They do *not* have hardcoded instructions on when to read a 10-K. 
3. **Dynamic Invocation:** Instead of a pipeline, a Bull agent might decide: *"To support my growth thesis, I need to check R&D spending. I will invoke the `SEC_Forensic_Auditor_Skill` specifically for the R&D section of the 10-K."*

## Rationale
This architecture allows us to "start fresh" with a bi-directional brain (the Personas) while still having the "hands" (the Skills) that we know already work. By treating the old specs as **Reference Capabilities**, we prevent the new system from becoming a linear sequence of steps. The system becomes truly agentic because the agents *choose* which skills to use based on the adversarial debate, rather than being forced through a pre-defined pipeline.

## References
This ADR explicitly references the domain logic found in the following legacy specifications:
- `helios/extraction_engine/agent_specs/sec_10k.md` -> Seed for `Financial_Extraction_Skill`
- `helios/extraction_engine/agent_specs/earnings_calls_analysis.md` -> Seed for `Sentiment_Auditor_Skill`
- `helios/extraction_engine/agent_specs/market_analysis.md` -> Seed for `Competitor_Benchmarking_Skill`

## Consequences
- **Positive:** Preserves the "precision" of the old extraction logic; enables bi-directional agents to share a common toolset; removes the "pipeline bias."
- **Negative:** Requires a refactoring phase to convert Markdown-based prompts into ADK-compatible tools/functions.
