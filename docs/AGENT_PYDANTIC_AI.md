# Pydantic AI Agent Architecture & Harness Specification

## 1. Overview & Purpose

In the Spark platform, **Pydantic AI** is the core agent intelligence framework. While Temporal guarantees durable execution across system crashes, Pydantic AI is responsible for:

- Interpreting user prompts and understanding developer intent.
- Formulating structured plans and milestones.
- Selecting appropriate tools (shell, filesystem, web research).
- Validating tool arguments and structured responses with strict **Pydantic V2** schemas.
- Observing tool execution results (stdout, stderr, exit codes) and iteratively self-correcting errors.
- Delegating specialized tasks to SubAgents.
- Managing context window limits through compaction and persistent conversation memory.

---

## 2. Pydantic AI Harness Integration

Spark leverages the **Pydantic AI Harness** architecture, providing prebuilt agent capabilities for software engineering workspaces:

```
┌──────────────────────────────────────────────────────────────────┐
│                     PYDANTIC AI AGENT HARNESS                    │
├──────────────────┬───────────────────────────────────────────────┤
│ Capability       │ Specific Responsibility                       │
├──────────────────┼───────────────────────────────────────────────┤
│ Coder            │ Code creation, refactoring, and diff-editing  │
│ Planning         │ Multi-step goal decomposition & milestone tracking │
│ Shell            │ Safe command execution in isolated sandboxes  │
│ FileSystem       │ Workspace file inspection, creation, editing  │
│ SubAgents        │ Delegation to specialized worker agents       │
│ Compaction       │ Automatic token pruning & context summarization│
│ Memory           │ Cross-session project knowledge persistence   │
└──────────────────┴───────────────────────────────────────────────┘
```

---

## 3. Dependency Injection Architecture

Pydantic AI utilizes typed dependency injection via `RunContext[SparkAgentDeps]`. This ensures that tools have access to the sandbox runtime, the event publisher, and run metadata without relying on global state.

```python
"""
spark/agent/dependencies.py
Type-safe runtime dependencies injected into all Pydantic AI tools.
"""

from dataclasses import dataclass
from typing import Optional
from spark.services.sandbox_manager import SandboxInstance
from spark.services.redis_streaming import RedisStreamPublisher


@dataclass
class SparkAgentDeps:
    """Dependencies injected into Pydantic AI agent tools."""
    run_id: str
    workspace_id: str
    sandbox: SandboxInstance
    publisher: RedisStreamPublisher
    user_id: str
    max_tokens_budget: int = 128000
```

---

## 4. Type-Safe Tool Implementations

All tool operations execute **strictly inside the isolated sandbox** over RPC/HTTP/WS. Tools **never** execute on the host machine running the Agent Worker or FastAPI server.

```python
"""
spark/agent/tools.py
Pydantic AI tool definitions with strict Pydantic validation.
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from pydantic_ai import RunContext
from spark.agent.dependencies import SparkAgentDeps


# --- Tool Input / Output Schemas ---

class CommandResult(BaseModel):
    command: str
    exit_code: int
    stdout: str
    stderr: str
    duration_ms: int


class FileInfo(BaseModel):
    name: str
    path: str
    size_bytes: int
    is_dir: bool


class EditFileResult(BaseModel):
    path: str
    success: bool
    replacements_made: int
    message: str


# --- Tool Functions ---

async def execute_command(
    ctx: RunContext[SparkAgentDeps],
    command: str = Field(description="The shell command to execute in the workspace sandbox."),
    timeout_seconds: int = Field(default=120, description="Max execution time in seconds."),
    working_directory: str = Field(default="/workspace", description="Working directory."),
) -> CommandResult:
    """
    Executes an arbitrary shell command (bash, python, node, git) inside the isolated sandbox.
    Streams incremental stdout/stderr to Redis for background telemetry and audit logging.
    """
    deps = ctx.deps

    # Emit tool.started event
    await deps.publisher.publish(
        run_id=deps.run_id,
        event_type="tool.started",
        payload={"tool": "execute_command", "command": command, "cwd": working_directory},
    )

    # Callback to record command execution chunks for telemetry
    async def on_stdout_chunk(chunk: str):
        await deps.publisher.publish(
            run_id=deps.run_id,
            event_type="tool.output",
            payload={"stream": "stdout", "text": chunk},
        )

    async def on_stderr_chunk(chunk: str):
        await deps.publisher.publish(
            run_id=deps.run_id,
            event_type="tool.output",
            payload={"stream": "stderr", "text": chunk},
        )

    # Execute inside isolated sandbox
    res = await deps.sandbox.run_command(
        command=command,
        timeout=timeout_seconds,
        cwd=working_directory,
        on_stdout=on_stdout_chunk,
        on_stderr=on_stderr_chunk,
    )

    # Emit tool.completed event
    await deps.publisher.publish(
        run_id=deps.run_id,
        event_type="tool.completed",
        payload={"tool": "execute_command", "exit_code": res.exit_code},
    )

    return CommandResult(
        command=command,
        exit_code=res.exit_code,
        stdout=res.stdout,
        stderr=res.stderr,
        duration_ms=res.duration_ms,
    )


async def read_file(
    ctx: RunContext[SparkAgentDeps],
    path: str = Field(description="Relative or absolute path to the file in /workspace."),
    start_line: Optional[int] = Field(default=None, description="Start line number (1-indexed)."),
    end_line: Optional[int] = Field(default=None, description="End line number (inclusive)."),
) -> str:
    """Reads the contents of a file from the workspace filesystem."""
    deps = ctx.deps
    content = await deps.sandbox.read_file(path=path, start_line=start_line, end_line=end_line)
    return content


async def write_file(
    ctx: RunContext[SparkAgentDeps],
    path: str = Field(description="File path in /workspace to create or overwrite."),
    content: str = Field(description="Full text contents to write into the file."),
) -> str:
    """Creates or overwrites a file in the workspace."""
    deps = ctx.deps
    await deps.sandbox.write_file(path=path, content=content)

    # Publish file modification event
    await deps.publisher.publish(
        run_id=deps.run_id,
        event_type="artifact.created",
        payload={"path": path, "size_bytes": len(content.encode("utf-8"))},
    )
    return f"File '{path}' successfully written ({len(content)} characters)."


async def edit_file(
    ctx: RunContext[SparkAgentDeps],
    path: str = Field(description="Path to the file to edit."),
    target: str = Field(description="Exact contiguous block of code to find and replace."),
    replacement: str = Field(description="New block of code to replace the target."),
) -> EditFileResult:
    """
    Performs a single surgical search-and-replace edit on an existing workspace file.
    Target must match existing file content exactly, including whitespace.
    """
    deps = ctx.deps
    result = await deps.sandbox.edit_file(path=path, target=target, replacement=replacement)
    
    await deps.publisher.publish(
        run_id=deps.run_id,
        event_type="artifact.modified",
        payload={"path": path, "replacements": result.replacements_made},
    )
    return result


async def list_files(
    ctx: RunContext[SparkAgentDeps],
    directory: str = Field(default=".", description="Directory path relative to /workspace."),
    recursive: bool = Field(default=False, description="Whether to recurse into subdirectories."),
) -> List[FileInfo]:
    """Lists files and directories within the workspace."""
    deps = ctx.deps
    return await deps.sandbox.list_files(directory=directory, recursive=recursive)
```

---

## 5. Agent Definition & System Prompt

The agent is instantiated using Pydantic AI's `Agent` class, configuring tools and system instructions:

```python
"""
spark/agent/pydantic_agent.py
Main Pydantic AI agent instance and system prompt configuration.
"""

from pydantic_ai import Agent
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.models.openai import OpenAIModel
from spark.agent.dependencies import SparkAgentDeps
from spark.agent.tools import (
    execute_command,
    read_file,
    write_file,
    edit_file,
    list_files,
)

SYSTEM_PROMPT = """\
You are Spark, an autonomous AI software engineer and technical workspace assistant.
You operate within a secure Linux container sandbox with access to a persistent workspace at `/workspace`.

### CORE OPERATING PRINCIPLES:
1. **Explore Before Acting:** Inspect files and directory trees using `list_files` and `read_file` before making modifications.
2. **Deterministic Edits:** Use `edit_file` for targeted changes. Ensure `target` exactly matches existing whitespace and indentation. Use `write_file` for creating new files.
3. **Validate Code:** Always execute tests or run scripts (`execute_command`) after making changes to verify correctness.
4. **Self-Correction:** If a command fails (exit code != 0), examine stderr, adjust your approach, and try again.
5. **No Hallucinations:** Never assume third-party packages or system dependencies are pre-installed. Install needed packages via `pip install`, `npm install`, or `apt` as required.
6. **User Visibility:** Provide concise, informative explanations of your plan and accomplishments.
"""

def create_spark_agent(model_name: str = "claude-3-5-sonnet-20241022") -> Agent[SparkAgentDeps, str]:
    """Factory function creating a configured Pydantic AI Agent."""
    
    # Model selection (supports Claude 3.5 Sonnet, GPT-4o, etc.)
    if "claude" in model_name:
        model = AnthropicModel(model_name)
    else:
        model = OpenAIModel(model_name)

    agent = Agent(
        model=model,
        deps_type=SparkAgentDeps,
        result_type=str,
        system_prompt=SYSTEM_PROMPT,
    )

    # Register tools
    agent.tool(execute_command)
    agent.tool(read_file)
    agent.tool(write_file)
    agent.tool(edit_file)
    agent.tool(list_files)

    return agent
```

---

## 6. Real-Time Token Streaming

To deliver an interactive user experience, Pydantic AI streams the agent's textual response chunks directly to Redis Streams as they are generated by the LLM:

```python
"""
Streaming token chunks from Pydantic AI to Redis Streams.
"""

async def stream_agent_response(
    agent: Agent[SparkAgentDeps, str],
    prompt: str,
    deps: SparkAgentDeps,
) -> str:
    """Runs agent with streaming text tokens emitted to Redis."""
    full_response_text = ""

    async with agent.run_stream(prompt, deps=deps) as response:
        async for delta in response.stream_text(delta=True):
            full_response_text += delta
            # Publish token chunk to Redis for SSE delivery to frontend
            await deps.publisher.publish(
                run_id=deps.run_id,
                event_type="assistant.delta",
                payload={"text": delta},
            )

    return full_response_text
```

---

## 7. Context Management & Window Compaction

Coding tasks generate extensive command outputs, large files, and lengthy conversation turns. If left unmanaged, this exhausts the LLM's context window.

Spark employs a two-tier compaction strategy:
1. **Command Output Truncation:** Large command outputs are truncated if they exceed 500 lines or 30KB. The full output is written to a temporary log file in `/workspace/.spark/logs/` and a summary with head/tail lines is returned to the model.
2. **Conversation History Compaction:** When message tokens exceed 70% of the model's budget, the Pydantic AI Harness triggers an automated summarization turn:
   - Older tool interactions and assistant thoughts are collapsed into a structured markdown recap:
     - *Completed tasks and milestones.*
     - *Key files created or edited.*
     - *Unresolved errors or next steps.*
   - The compact summary replaces the older turns in `message_history`, preserving context while freeing up token capacity.
