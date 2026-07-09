# Architecture proposals for a Go package with an OS signal adapter and event remapping

## Variables

* `executionMode`: ask the user at the beginning of the run whether this prompt is being executed in `planning` mode
  or `default` mode.
* `outputDir`: build this value from `executionMode`:
* * if `executionMode` is `default`, use `architecture-proposals/no-planning/complex`
* * if `executionMode` is `planning`, use `architecture-proposals/planning/complex`

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

### Required architecture variants

At minimum, consider variants from these families:

* Single-consumer channel API. The caller starts the listener and receives mapped events from a receive-only channel.
* Callback/handler-based API. Users register handlers that are invoked when a mapped signal arrives.
* Context-oriented API. A design based on `context.Context`, cancellation, and/or `signal.NotifyContext`.

For each variant, include:
* short description
* intended use cases
* public API sketch
* core internal components
* concurrency model
* lifecycle state machine
* cancellation/stop behavior
* event delivery semantics
* backpressure behavior
* ordering guarantees
* channel ownership and closing rules, if channels are used
* error model
* validation rules
* testing strategy
* pros
* cons
* risks
* usage limitations

### Architecture issues that must be addressed

#### Concurrency safety. 

* shared state ownership
* mutex/channel/atomic usage
* safe publication of state
* concurrent Start, Stop, Subscribe, Unsubscribe, or equivalent calls
* goroutine lifecycle
* prevention of goroutine leaks
* safe channel closing rules

#### Lifecycle

Define the state machine. For example: New, Running, Stopping, Stopped, Closed

Describe which public methods are valid in each state and what errors are returned otherwise.

#### Signal registration and cleanup

Describe how OS signals are registered and unregistered.
Discuss whether the design uses:
* `signal.Notify`
* `signal.NotifyContext`
* a custom signal source abstraction for testability

Mention cleanup requirements such as stopping signal delivery when the listener is stopped.

#### Mapping validation

Describe how the package validates the mapping:
* empty mapping
* nil mapping
* duplicate signals if using slice-based configuration
* unsupported platform-specific signals
* unmapped signals
* copying user-provided maps to prevent external mutation
* whether multiple signals may map to the same user event

#### Delivery semantics

Define:
* whether each event is delivered to one consumer or all subscribers
* whether delivery is best-effort or guaranteed while listener is running
* what happens when a receiver is slow
* whether events may be dropped
* whether event ordering is preserved
* whether duplicate OS signals produce duplicate events
* what happens during shutdown if events are buffered

#### Backpressure

Discuss at least these options where relevant:
* blocking delivery
* bounded buffers
* dropping newest event
* dropping oldest event
* returning delivery errors
* isolating slow subscribers

#### Errors

Propose typed/sentinel errors where useful. Use idiomatic Go error handling. Avoid over-engineered error hierarchies.

#### API design

The API should follow idiomatic Go design. Consider:
* small interfaces
* clear ownership
* context-first cancellation where appropriate
* zero-value usability, if practical
* constructor validation
* option pattern only if it materially improves usability
* avoiding unnecessary global state
* avoiding reflection
* avoiding unsafe
* avoiding hidden goroutines that cannot be stopped
* minimal exported surface area

#### Testing strategy

Do not write full tests, but describe how the design would be tested. Cover:
* deterministic unit tests with fake signal source
* lifecycle tests
* concurrent Start/Stop tests
* subscriber add/remove tests
* slow subscriber tests
* cancellation tests
* race detector expectations
* real signal integration tests where appropriate
* platform-specific test constraints

### Style requirements

Pay special attention to:
* package naming
* exported type and method names
* comments for exported identifiers
* interface size
* error naming
* context usage
* channel ownership
* mutex usage
* avoiding stutter
* avoiding premature abstraction
* keeping API contracts simple

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

## Final checklist

Confirm that the proposal covers:
* concurrency safety
* lifecycle
* cancellation
* signal cleanup
* mapping validation
* delivery semantics
* backpressure
* testing
* errors
* platform notes
* style guide alignment
* Expected quality bar

The final document should be specific enough that another engineer could implement the package from the chosen architecture without needing to reinterpret lifecycle, event delivery, cancellation, or concurrency semantics.

Avoid vague statements such as "use mutexes for safety" without explaining which state is protected and who owns channel closing.

Avoid generic architecture filler. Prefer concrete Go API contracts and explicit trade-offs.