# Frontend Architecture & UI Design Specification

## 1. Overview & Technology Choices

The Spark frontend is an interactive, multi-pane developer workspace designed as a single-page application (SPA). It provides users with full visibility into the AI agent's execution, task milestones, file changes, and live responses.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                SPARK WORKSPACE UI                               │
├────────────────────┬───────────────────────────────┬────────────────────────────┤
│ Left Panel (20%)   │ Center Panel (50%)            │ Right Panel (30%)          │
├────────────────────┼───────────────────────────────┼────────────────────────────┤
│ • File Tree        │ • Monaco Code Editor          │ • Conversational Timeline  │
│ • Workspace Tabs   │ • Side-by-Side Diff Viewer    │ • Task Plan & Milestones   │
│ • Generated Assets │ • Full-Height File Inspector  │ • Human Approval Banners   │
│                    │   (No Terminal Displayed)     │ • Chat Input Box           │
└────────────────────┴───────────────────────────────┴────────────────────────────┘
```

### Technology Rationale: Vite + React 19 vs. Next.js
As detailed in the architecture evaluation, **Vite + React + TypeScript** was selected over Next.js because:
1. **Application Nature:** Spark is a desktop-style workspace with dynamic state (Monaco code editor, task plan milestones, and real-time SSE streams). It requires no search engine optimization (SEO) or public Server-Side Rendering (SSR).
2. **Clean Separation of Concerns:** FastAPI serves as the sole, authoritative backend handling authentication, database transactions, Temporal workflows, and SSE event dispatch. Next.js would introduce a redundant Node.js runtime, complicating deployment and introducing dual-backend ambiguities.
3. **Transparent Execution (Zero Terminal):** Code execution and shell environments are **completely transparent to the user**. The user does not interact with a terminal or shell in any way. All code executions happen autonomously in background sandboxes. Users simply ask the agent to perform tasks and inspect the resulting files.

---

## 2. Directory Structure

The frontend repository is structured within `Frontend/src/` as follows:

```
Frontend/src/
├── components/
│   ├── chat/
│   │   ├── ChatTimeline.tsx         # Message stream with Markdown & token appending
│   │   ├── MessageBubble.tsx        # Individual user / assistant message
│   │   └── ThinkingIndicator.tsx    # Smooth indeterminate loading status
│   ├── plan/
│   │   ├── TaskPlanTimeline.tsx     # Structured milestones & plan progression
│   │   ├── MilestoneItem.tsx        # Step indicator (pending, active, completed)
│   │   └── ApprovalModal.tsx        # Human-in-the-loop action confirmation
│   ├── editor/
│   │   ├── MonacoWorkspace.tsx      # Monaco editor wrapper with file tabs
│   │   ├── DiffViewer.tsx           # Side-by-side diff viewer for file edits
│   │   └── EditorToolbar.tsx        # Save, format, and path breadcrumbs
│   ├── explorer/
│   │   ├── FileTree.tsx             # Nested directory tree with icons
│   │   └── FileNode.tsx             # Individual file/folder row
│   └── layout/
│       ├── Header.tsx               # Workspace title, status pill, connection badge
│       ├── ResizablePanels.tsx      # Multi-pane drag-to-resize splitters
│       └── Sidebar.tsx              # Workspace switcher and settings
├── hooks/
│   ├── useAgentStream.ts            # Resilient SSE consumer with Last-Event-ID
│   └── useWorkspaceFiles.ts         # REST queries for file hierarchy & content
├── store/
│   ├── useWorkspaceStore.ts         # Active workspace, open file tabs, active editor
│   └── useAgentStore.ts             # Runs, streaming tokens, plan steps, status
├── types/
│   ├── events.ts                    # Strongly typed SparkEvent interfaces
│   ├── workspace.ts                 # File node, workspace, and artifact types
│   └── api.ts                       # REST request and response DTO types
├── App.tsx                          # Root workspace container
└── main.tsx                         # Vite application entry point
```

---

## 3. Resilient SSE Stream Consumer (`useAgentStream`)

To handle network fluctuations, laptop sleep cycles, and browser tab reloads, Spark implements a custom hook using the `@microsoft/fetch-event-source` pattern:

```typescript
/**
 * Frontend/src/hooks/useAgentStream.ts
 * Resilient SSE client with automatic reconnection and Last-Event-ID replay.
 */

import { useEffect, useRef, useState, useCallback } from "react";
import { SparkEvent, SparkEventType } from "../types/events";
import { useAgentStore } from "../store/useAgentStore";

interface UseAgentStreamOptions {
  runId: string | null;
  onCompleted?: () => void;
  onError?: (err: Error) => void;
}

export function useAgentStream({ runId, onCompleted, onError }: UseAgentStreamOptions) {
  const [isConnected, setIsConnected] = useState(false);
  const [isReconnecting, setIsReconnecting] = useState(false);
  const lastEventIdRef = useRef<string>("0");
  const abortControllerRef = useRef<AbortController | null>(null);

  const {
    appendDelta,
    updateStatus,
    setPlan,
    setApprovalRequired,
    setRunStatus,
  } = useAgentStore();

  const handleEvent = useCallback((event: SparkEvent) => {
    lastEventIdRef.current = event.id;

    switch (event.type) {
      case "assistant.delta":
        appendDelta(event.payload.text);
        break;

      case "agent.status":
        updateStatus(event.payload.status, event.payload.message);
        break;

      case "agent.plan":
        setPlan(event.payload.steps);
        break;

      case "tool.output":
        // Transparent execution: raw tool outputs are recorded for internal error handling
        // without exposing a raw terminal emulator to the user
        break;

      case "approval.required":
        setApprovalRequired({
          stepId: event.payload.step_id,
          command: event.payload.command,
          reason: event.payload.reason,
        });
        break;

      case "run.completed":
        setRunStatus("completed");
        if (onCompleted) onCompleted();
        break;

      case "run.failed":
        setRunStatus("failed");
        break;

      default:
        console.debug("Unhandled event:", event);
    }
  }, [appendDelta, updateStatus, setPlan, appendTerminalOutput, setApprovalRequired, setRunStatus, onCompleted]);

  useEffect(() => {
    if (!runId) return;

    let isMounted = true;
    abortControllerRef.current = new AbortController();

    async function connectSSE() {
      try {
        setIsReconnecting(true);
        const headers: Record<string, string> = {
          "Accept": "text/event-stream",
        };

        if (lastEventIdRef.current && lastEventIdRef.current !== "0") {
          headers["Last-Event-ID"] = lastEventIdRef.current;
        }

        const response = await fetch(`/api/runs/${runId}/events`, {
          headers,
          signal: abortControllerRef.current?.signal,
        });

        if (!response.ok || !response.body) {
          throw new Error(`SSE failed with HTTP status ${response.status}`);
        }

        setIsConnected(true);
        setIsReconnecting(false);

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (isMounted) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n\n");
          buffer = lines.pop() || ""; // Keep remainder in buffer

          for (const block of lines) {
            if (!block.trim() || block.startsWith(":")) continue; // Skip comments/keepalives

            let eventType = "message";
            let dataStr = "";
            let eventId = "";

            for (const line of block.split("\n")) {
              if (line.startsWith("event: ")) eventType = line.slice(7).trim();
              if (line.startsWith("data: ")) dataStr = line.slice(6).trim();
              if (line.startsWith("id: ")) eventId = line.slice(4).trim();
            }

            if (dataStr) {
              try {
                const parsed: SparkEvent = JSON.parse(dataStr);
                if (eventId) parsed.id = eventId;
                handleEvent(parsed);
              } catch (e) {
                console.error("Failed to parse SSE JSON frame:", e, dataStr);
              }
            }
          }
        }
      } catch (err: any) {
        if (err.name === "AbortError") return;
        console.warn("SSE disconnected, scheduling retry...", err);
        setIsConnected(false);
        setIsReconnecting(true);

        if (isMounted) {
          // Exponential backoff reconnect
          setTimeout(connectSSE, 3000);
        }
      }
    }

    connectSSE();

    return () => {
      isMounted = false;
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    };
  }, [runId, handleEvent]);

  return { isConnected, isReconnecting };
}
```

---

## 4. Key UI Components & Interactions

### 1. Task Plan & Milestones Component (`TaskPlanTimeline.tsx`)
Rather than dumping raw chain-of-thought into the chat window, the UI displays an intuitive, collapsible task roadmap:
- **Pending:** Dimmed bullet.
- **Active / Executing:** Animated spinner with elapsed timer.
- **Completed:** Green checkmark with duration badge.
- **Failed:** Red error badge with collapsible error traceback.

### 2. Code Editor with Live Updates (`MonacoWorkspace.tsx`)
- Embeds the official `@monaco-editor/react`.
- When an `artifact.created` or `artifact.modified` event arrives, the active tab automatically reloads or displays a side-by-side **Monaco Diff Viewer**, showing the exact edits performed by the agent.
- Supports syntax highlighting for 50+ languages, auto-indentation, and dark/light themes.

### 3. File Explorer & Artifacts Inspector (`FileTree.tsx`)
- Displays workspace project files and generated artifacts in a clean hierarchical tree.
- When the agent generates or updates files in `/workspace` (via `artifact.created` or `artifact.modified` events), the tree dynamically updates with badges highlighting modified files.
- Users click any file in the tree to inspect it in the Monaco editor.
- **Zero Terminal Exposure:** No terminal emulator or command-line console is displayed anywhere in the UI. Low-level command executions (`pip install`, `npm build`, python script execution) occur completely transparently within the background sandbox, keeping the user interface clean, professional, and accessible.

### 4. Human Approval Modal (`ApprovalModal.tsx`)
When the Temporal workflow detects a sensitive operation (e.g., deleting directories, pushing to git remote), execution pauses and triggers an interactive banner:
- Shows the exact command proposed by Pydantic AI.
- Provides **"Approve"** and **"Reject"** buttons.
- On click, issues `POST /api/runs/{run_id}/input` to signal Temporal to resume execution.
