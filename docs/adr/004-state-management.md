# ADR 004: Adversarial State & Memory Management

**Date:** 2026-05-23
**Status:** Proposed

## Context
In a bi-directional system where a "Bull" and "Bear" agent talk back and forth, the system needs a way to maintain the "Debate History." Because we are building a "scalable production software" (ADR 002), this state cannot live in the memory of a single Python process; it must be persistent and accessible by any agent worker.

## Options Considered
1. **In-Agent Context:** Passing the entire history in every message. (Expensive and hits context limits).
2. **Short-Term Redis Memory:** Storing the active debate in Redis. (Fast, but lost if the system reboots).
3. **Stateful Graph Persistence (ADK Native):** Using the ADK's built-in state management backed by a database (PostgreSQL/Cloud SQL).

## Decision
We will use **ADK Stateful Workflows with a PostgreSQL/Cloud SQL backend**:
1. **Debate State:** Every "back and forth" turn is persisted to a central database. 
2. **Checkpointing:** The ADK will "checkpoint" the debate after every agent response. 
3. **Session Resumption:** If a Bear agent crashes or the UI is refreshed, the system reads the last checkpoint from the DB and resumes the debate exactly where it left off.

## Rationale
This is the only way to achieve the "Scalable Production" goal. By using a persistent database for the debate state, we gain **Auditability** (you can see the history of every debate ever held) and **Fault Tolerance**. Passing context back and forth in messages (Option 1) is too fragile for a complex bi-directional system.

## Consequences
- **Positive:** Perfect debuggability (you can "time travel" through the debate); high reliability.
- **Negative:** Adds a database dependency (PostgreSQL) to our local Docker Compose setup.
