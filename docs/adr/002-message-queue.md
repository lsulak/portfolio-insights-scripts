# ADR 002: Message Queue & Local-First Strategy

**Date:** 2026-05-23
**Status:** Proposed

## Context
The new Helios system requires a decoupled, event-driven architecture to support asynchronous agent-to-agent task dispatching and to queue real-time updates for the UI. The user explicitly required a solution that is initially local and easily Dockerized, but provides a clear migration path to a scalable cloud-based solution.

## Options Considered
1. **Google Cloud Pub/Sub Emulator:** Running the GCP emulator in Docker.
2. **RabbitMQ:** Excellent for Docker, translates to managed cloud services, but high protocol complexity.
3. **Redis (Pub/Sub & Streams):** Extremely lightweight, trivial to Dockerize, blazing fast, but ephemeral.
4. **Broker Abstraction Layer:** Designing the system to use interfaces for queues, allowing underlying technologies to swap between environments.

## Decision
We will adopt a **Broker Abstraction Layer with a Local-First Docker Strategy**:

1. **The Abstraction:** All agent-to-agent communication and UI streaming will pass through a generic `EventBus` / `TaskDispatcher` interface in the code, never referencing the broker directly.
2. **Local Environment (Docker Compose):** We will use **Redis** exclusively for everything. It is trivial to run in Docker (`redis:alpine`), requires zero configuration, and provides sub-millisecond latency for both task routing and UI token streaming.
3. **Cloud Production Environment:** When migrating to the cloud, the `TaskDispatcher` implementation will be swapped to **Google Cloud Pub/Sub** for durable, high-scale agent orchestration, while keeping a managed **Redis** instance (e.g., Memorystore) exclusively for the low-latency UI streaming.

## Rationale
Building directly against Google Cloud Pub/Sub from day one, even with an emulator, introduces unnecessary friction and heavy containers to local development. Redis is the industry standard for lightweight, local Docker setups. By enforcing a strict abstraction layer, we get the best of both worlds: a frictionless, single-container (Redis) local development experience, and the ability to unlock Google's serverless, zero-ops Pub/Sub architecture for heavy lifting when moving to production. 

## Consequences
- **Positive:** Trivial local setup via a simple `docker-compose.yml`; no vendor lock-in for the core agent logic; highly responsive local testing.
- **Negative:** Requires disciplined engineering upfront to design the `EventBus` interface so that it successfully hides the differences between Redis Streams and Google Pub/Sub.
