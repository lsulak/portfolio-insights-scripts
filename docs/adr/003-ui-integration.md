# ADR 003: UI Integration Transport Protocol

**Date:** 2026-05-23
**Status:** Proposed

## Context
The custom frontend for Helios needs to display the real-time "Bull vs. Bear" agent debates and stream LLM text generation token-by-token. Additionally, the user must be able to interrupt or inject prompts into the ongoing debate. We need to decide the transport protocol connecting the Python backend (consuming Redis queues) to the frontend client.

## Options Considered
1. **Server-Sent Events (SSE):** Unidirectional HTTP streaming protocol.
2. **WebSockets:** Full-duplex (bi-directional) stateful TCP protocol.
3. **HTTP Long Polling:** Legacy workaround for streaming.

## Comparison

| Option | Pros | Cons | Recommendation |
|--------|------|------|----------------|
| **Server-Sent Events (SSE)** | Lightweight standard HTTP, native automatic reconnection, easy to route through load balancers/Docker Nginx. | Unidirectional (server-to-client only), requires HTTP/2 for >6 connections. | **Recommended.** The industry standard for LLM token streaming. |
| **WebSockets** | Full-duplex communication allows streaming and user interrupts on the same channel. | Highly stateful, requires sticky sessions for scaling, complex handshake and heartbeat management. | Not recommended due to infrastructure overhead for this use case. |
| **HTTP Long Polling** | Universal firewall/proxy compatibility. | High latency, massive server overhead for character-by-character token streaming. | Not recommended. |

## Decision
We will use a **Hybrid SSE + REST Pattern**:
1. **Downstream (Backend -> UI):** Use **Server-Sent Events (SSE)** to stream real-time agent states and LLM tokens. The backend will yield async generators directly from the Redis stream to the client.
2. **Upstream (UI -> Backend):** Use standard **REST POST requests** for user interactions (e.g., `POST /debate/interrupt` or `POST /debate/inject`). These endpoints will write the interrupt signal to the Redis queue, which the streaming agents monitor to halt or pivot their execution.

## Rationale
SSE aligns with the industry standard for LLM streaming (used by OpenAI, Anthropic, etc.). WebSockets, while offering bi-directionality, introduce massive infrastructure complexity regarding load balancing, sticky sessions, and connection lifecycle management in Docker and Cloud environments. By decoupling the "read" stream (SSE) from the "write" actions (REST), we keep the architecture entirely stateless at the HTTP layer, relying on our Redis broker (from ADR 002) to maintain the state.

## Consequences
- **Positive:** Extremely scalable, stateless web tier; trivial to configure in Docker/Nginx or cloud load balancers; simple frontend implementation using the native browser `EventSource` API.
- **Negative:** Requires careful handling of Nginx proxy buffering (must be disabled for SSE routes) to ensure tokens stream smoothly.
