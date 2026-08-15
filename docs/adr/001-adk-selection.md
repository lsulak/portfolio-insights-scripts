# ADR 001: Agent Development Kit (ADK) Selection

**Date:** 2026-05-23
**Status:** Proposed

## Context
The new Helios architecture requires a scalable, bi-directional, and adversarial multi-agent system (e.g., a 'Bull' investment agent debating a 'Bear' investment agent). The system must support event-driven communication, deep observability for debugging complex agent interactions, and real-time UI integration. The user suggested evaluating Google's Agent Development Kit (ADK) (`google/adk-python`).

## Options Considered
1. **Google ADK (`google/adk-python`):** Code-first framework by Google with a graph-based workflow runtime and built-in UI.
2. **LangGraph:** Graph-based state machine framework.
3. **AutoGen (v0.4+):** Asynchronous actor-model framework.
4. **CrewAI:** Role-based, task-oriented framework.

## Comparison

| Option | Pros | Cons | Recommendation |
|--------|------|------|----------------|
| **Google ADK** | Code-first approach, graph-based Workflow Runtime handles routing/loops natively, Task API specifically designed for multi-turn agent delegation, **built-in Development UI (`adk web`)** for testing/debugging. | Newer ecosystem compared to some alternatives, potentially fewer third-party community examples. | **Recommended.** Perfectly aligns with the request for an ADK with built-in UI and bi-directional capabilities. |
| **LangGraph** | Native support for cyclic graphs, excellent tracing via LangSmith. | Requires third-party service (LangSmith) for best observability, UI is decoupled. | Strong alternative, but requires more manual wiring for UI. |
| **AutoGen** | Asynchronous actor model. | High architectural overhead for queue routing. | Not recommended for this specific graph/UI requirement. |
| **CrewAI** | Rapid prototyping. | Not designed for continuous bi-directional loops. | Not recommended. |

## Decision
We will use **Google's Agent Development Kit (ADK)** (`google/adk-python`) as the core framework.

## Rationale
Google's ADK is explicitly designed for the requirements of the new Helios system. Its **Workflow Runtime** provides the graph-based execution flows necessary to model a multi-turn, adversarial debate (loops, state management, routing). Crucially, its **Task API** offers structured agent-to-agent delegation, and it ships with a **built-in development UI (`adk web`)** and CLI for immediate observability and debugging. This significantly reduces the overhead of building a custom UI just to trace why the Bull and Bear agents disagreed.

## Consequences
- **Positive:** Out-of-the-box UI for agent debugging; native graph routing for the Bull vs. Bear debate; code-first approach aligns with scalable production software practices.
- **Negative:** May require adapting to Google's specific workflow semantics; ecosystem is evolving.
