# Spark: Cloud-Native Autonomous AI Agent Platform

**Spark** is a multi-tenant, cloud-based autonomous AI agent platform designed for arbitrary code execution, multi-step software engineering, and interactive developer workflows. Inspired by platforms such as Gemini Spark, Devin, and Manus, Spark provides developers with an intelligent coding partner operating inside secure, persistent workspaces.

---

## 🛠️ The Spark Technology Stack

Based on extensive architectural analysis and evaluation, Spark is built on five core pillars:

1. **[Temporal](https://temporal.io):** Durable workflow orchestration, deterministic state recovery across worker crashes, activity retries, heartbeats, and human-in-the-loop approvals.
2. **[FastAPI](https://fastapi.tiangolo.com):** Asynchronous Python backend, non-blocking API gateway, user authentication, session management, and SSE streaming.
3. **[Vite + React 19 + TypeScript](https://vitejs.dev):** High-performance interactive single-page application (SPA) with Monaco code editor, task plan milestones tracker, file tree explorer, and real-time event streaming.
4. **[Redis Streams](https://redis.io/docs/latest/develop/use-cases/streaming/) + [SSE](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events):** Ordered, persistent event buffering with Server-Sent Events, supporting seamless browser disconnect/reconnect tolerance via `Last-Event-ID`.
5. **[Pydantic AI](https://ai.pydantic.dev):** Type-safe agent framework with Pydantic validation, planning harness, file/shell tools, subagent delegation, and context compaction.

---

## 📖 Comprehensive Architectural Documentation

Complete technical specifications and system designs are available in the [`docs/`](./docs) directory:

- 📋 **[Documentation Hub & Executive Overview](./docs/README.md)** — High-level introduction, topology diagrams, and implementation roadmap.
- 🏛️ **[Master Architecture Blueprint (`ARCHITECTURE.md`)](./docs/ARCHITECTURE.md)** — Full system topology, decoupled lifecycles, state tiers, and sequence flows.
- ⏱️ **[Temporal Workflow Engine (`TEMPORAL_WORKFLOWS.md`)](./docs/TEMPORAL_WORKFLOWS.md)** — Workflow state machines, activities, crash resilience, timeouts, and human-in-the-loop signals.
- 🤖 **[Pydantic AI Agent Architecture (`AGENT_PYDANTIC_AI.md`)](./docs/AGENT_PYDANTIC_AI.md)** — Agent harness, type-safe tools, sandbox redirection, context compaction, and token streaming.
- 📡 **[Redis Streams & SSE Streaming Protocol (`EVENT_STREAMING.md`)](./docs/EVENT_STREAMING.md)** — Stream topologies, event schemas, disconnect handling, and `Last-Event-ID` replay.
- 🔒 **[Sandbox Isolation & Security (`SANDBOX_AND_SECURITY.md`)](./docs/SANDBOX_AND_SECURITY.md)** — E2B, gVisor, Firecracker comparisons, volume persistence, egress proxying, and workspace concurrency locking.
- 🗄️ **[API Contracts & Database Schemas (`API_AND_DATABASE.md`)](./docs/API_AND_DATABASE.md)** — PostgreSQL relational schema DDL, FastAPI REST routes, SSE contracts, and Pydantic DTOs.
- 💻 **[Frontend Architecture & UI Design (`FRONTEND_DESIGN.md`)](./docs/FRONTEND_DESIGN.md)** — React 19 component layout, resilient SSE hook, Monaco editor, task plan milestones, and transparent terminal-free workspace.
- 💬 **Architectural Discussions:** [chatgpt.chat.md](./docs/chatgpt.chat.md) & [gemini.chat.md](./docs/gemini.chat.md).

---

## 🏗️ Quick Architectural Overview

```mermaid
flowchart LR
    Browser["React 19 SPA\n(Chat + Task Plan + Monaco)"]
    API["FastAPI Gateway\n(Async / Non-Blocking)"]
    Temporal["Temporal Cluster\n(Durable Workflows)"]
    Worker["Agent Worker\n(Pydantic AI + Tools)"]
    Redis[("Redis Streams\n(Event Buffer)")]
    Postgres[("PostgreSQL\n(Metadata & History)")]
    Sandbox["Isolated Sandbox\n(E2B / Firecracker)"]
    Storage[("Persistent Storage\n(/workspace volume)")]

    Browser -->|1. Submit Prompt| API
    Browser -->|3. SSE Stream (Last-Event-ID)| API
    API -->|2. Start Workflow| Temporal
    Temporal <-->|Dispatch Activities| Worker
    Worker -->|Agent Logic| Sandbox
    Sandbox <-->|Mount| Storage
    Worker -->|Publish Events| Redis
    Redis -->|Forward Events| API
    Worker -->|Persist Runs/Msgs| Postgres
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.12+
- Node.js 20+ & npm
- Docker / Docker Compose (for PostgreSQL, Redis, and Temporal development server)

### Running the Frontend
```bash
cd Frontend
npm install
npm run dev
```

For detailed backend setup, Temporal worker configuration, and sandbox provisioning, refer to the [System Architecture Guide](./docs/ARCHITECTURE.md).
