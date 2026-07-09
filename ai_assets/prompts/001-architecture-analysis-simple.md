# Architecture proposals for a Go package with an OS signal adapter and event remapping

## Variables

* `executionMode`: ask the user at the beginning of the run whether this prompt is being executed in `planning` mode
  or `default` mode.
* `outputDir`: build this value from `executionMode`:
* * if `executionMode` is `default`, use `architecture-proposals/no-planning/simple`
* * if `executionMode` is `planning`, use `architecture-proposals/planning/simple`

Before starting the analysis, explicitly ask the user which mode is being used: `planning` or `default`.
Do not continue until the user provides one of these values. Use the selected mode to build the `outputDir` path.

## Context

Language: Go, version 1.26  
OS: Unix

### Task

1. Design 3-4 architecture variants for a package that listens for OS signals and remaps them to user-defined events,
   then delivers those events to the caller or other listeners under concurrent use.
2. This task does not require writing production code. However, the proposed variants may include small code snippets
   that illustrate data contracts, public APIs, type usage, functions, methods, and similar concepts.
3. Each proposed architecture variant must include a description, a list of pros and cons, a list of risks, and a list
   of usage limitations.
4. The proposed variants must follow idiomatic Go, Effective Go principles, and best practices, including Google Go
   Style and Uber Go Style.

### Architecture requirements

1. The proposed variants must follow idiomatic Go and best practices, including Google Go Style and Uber Go Style.
2. The proposed variants must be concurrency-safe, data-race-safe, and free from memory leaks and goroutine leaks.
3. The proposed variants must be testable, and the architecture must explicitly account for testability by design.
4. The proposed variants must provide a sufficient level of abstraction for the user without exposing the internal
   mechanics of the `os/signal` package.
5. The implementation model must be concurrency-safe and must prevent data races.
6. The method that starts signal listening may be called successfully only once. Repeated calls must fail and notify the
   caller that listening is already active or has already been started. This lifecycle rule applies to a single created
   object for its entire lifetime. Internal state reset for a repeated start is not allowed.
7. It must be possible to cancel already running signal listening.
8. It must be possible to define a mapping between OS signals and user-defined values. For example:
   `syscall.SIGTERM -> lifecycle.EventGracefulShutdown`, `syscall.SIGUSR1 -> lifecycle.EventReloadLogs`, where the
   package user defines the `lifecycle.*` objects to be returned.

## Analysis process requirements

1. Do not search, inspect, or read files located in the root directory or any subdirectories. Existing information in
   the directory, subdirectories, or repository must not influence the analysis result. Writing the results to files and
   creating the required directories is allowed.
2. If any assumptions are used as the basis for an architecture variant, those assumptions must be stated explicitly in
   the resulting file.

## Output result requirements

1. The analysis results and proposed variants must be saved as a Markdown file in the `outputDir` directory.
2. The filename must consist of the model name and the effort level used to run this prompt. For example:
   `claude-code-opus-4.6-high.md`, `codex-5.5-xhigh.md`.
3. If the model and effort level cannot be determined, explicitly ask the user for the filename and provide the expected
   format as a reference. Validate the filename provided by the user against the required format. If the filename does
   not match the format, suggest a correction or ask the user to clarify the filename until it matches the required
   format.
4. The resulting file must include a short comparative table that analyzes the proposed variants against each other.
5. If possible, include the task execution time in the resulting file, broken down by phase, such as planning mode and
   default mode. Also include token usage broken down by type, such as input, output, cache, and so on.
6. Choose one recommended default architecture. Explain why it is the best default, when not to use it, which variant
   is best for simpler cases - which variant is best for highly concurrent pub/sub cases.