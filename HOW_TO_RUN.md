# Coding Agents Test: Go Signal Adapter

## Running architecture analysis tests

This section describes how to run the same architecture-analysis prompt in both planning mode and default mode.

Each run must be executed in a fresh new session with no previous context. Do not reuse an existing Codex, Claude Code,
or other AI coding-agent session between runs.

### General rules

1. Start a new AI coding-agent session.
2. Choose the model you want to test.
3. Use the same model and the same reasoning effort or thinking level for both runs:
    - one run with `executionMode: planning`
    - one run with `executionMode: default`
4. Use a fresh new session for every run.
5. Do not continue from a previous conversation, chat history, or restored context.
6. Paste the corresponding instruction from the sections below.
7. Let the agent create the output file according to the prompt instructions.

### Codex

For Codex, use the same model and the same `model_reasoning_effort` for both runs.

Use the same filename for both runs, because the output path is determined by `executionMode`.

#### Codex - planning run

Start a fresh new Codex session with the selected model and effort, then paste:

```text
Work on instructions at ai_assets/prompts/001-architecture-analysis-simple.md
Filename: codex-5.5-xhigh.md
executionMode: planning
```

Expected output directory:

```text
architecture-proposals/planning/simple
```

#### Codex - default run

Start another fresh new Codex session with the same model and effort, then paste:

```text
Work on instructions at ai_assets/prompts/001-architecture-analysis-simple.md
Filename: codex-5.5-xhigh.md
executionMode: default
```

Expected output directory:

```text
architecture-proposals/no-planning/simple
```

### Claude Code

For Claude Code, use the same model and the same thinking level for both runs.

Recommended setup:

* Planning run: start Claude Code in plan mode.
* Default run: start Claude Code in normal/default mode.
* Use the same model for both runs.
* Use a fresh new session for each run.

#### Claude Code - planning run

Start a fresh new Claude Code session in plan mode:

```bash
claude --permission-mode plan
```

Then paste:

```text
Work on instructions at ai_assets/prompts/001-architecture-analysis-simple.md
Filename: claude-code-sonnet-4.6-high.md
executionMode: planning
```

Expected output directory:

```text
architecture-proposals/planning/simple
```

#### Claude Code - default run

Start another fresh new Claude Code session in default mode:

```bash
claude
```

Then paste:

```text
Work on instructions at ai_assets/prompts/001-architecture-analysis-simple.md
Filename: claude-code-sonnet-4.6-high.md
executionMode: default
```

Expected output directory:

```text
architecture-proposals/no-planning/simple
```

### Other AI coding agents

For any other AI coding agent:

1. Start a fresh new session with no previous context.
2. Select the model.
3. Select the same reasoning effort, thinking level, or equivalent setting for both runs.
4. Run the planning case first or second - the order does not matter as long as sessions are isolated.
5. Use the same filename pattern for both runs.

#### Other agent - planning run

```text
Work on instructions at ai_assets/prompts/001-architecture-analysis-simple.md
Filename: <agent-model-effort>.md
executionMode: planning
```

Expected output directory:

```text
architecture-proposals/planning/simple
```

#### Other agent - default run

```text
Work on instructions at ai_assets/prompts/001-architecture-analysis-simple.md
Filename: <agent-model-effort>.md
executionMode: default
```

Expected output directory:

```text
architecture-proposals/no-planning/simple
```

### Important session isolation requirement

Every run must use a fresh new session without context.

Do not reuse:

* the same chat
* the same restored session
* previous task history
* previous architecture output
* previous plan-mode result
* previous default-mode result

This is required so that the planning-mode and default-mode outputs can be compared fairly.

