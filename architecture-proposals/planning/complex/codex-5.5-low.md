# OS Signal Adapter and Event Remapping Architectures

## Executive summary

This document proposes three concurrency-safe Go architectures for converting Unix OS signals into application-defined
events:

1. a single-consumer event channel;
2. registered callback handlers;
3. a context-oriented subscription API.

The single-consumer channel design is the recommended default. It has the smallest useful API, makes ownership clear,
preserves ordering, and composes naturally with a caller-owned event loop. The context-oriented design is simplest when
one mapped event only needs to cancel work. The callback design is best of these three for concurrent pub/sub, although
a production-grade event bus may be preferable when delivery guarantees, replay, or large subscriber counts are needed.

The designs deliberately hide `os/signal`. Each uses an internal signal-source seam so unit tests do not send real
process signals. Every created listener is start-once: after any `Start` attempt successfully transitions it out of
`New`, it can never return to `New`, including after cancellation or failure during the running phase.

## Shared contracts and assumptions

### Assumptions

- The package targets Go 1.26 on Unix-like systems.
- Package name `signalmap` is used below. A platform-specific application may choose a more domain-specific package name.
- `T` may be any type. The adapter transports values and does not compare them.
- Multiple signals may map to the same event.
- Each received, mapped signal represents one event. OS-level signal coalescing can occur before the package observes a
  signal, so the API cannot promise one event per signal sent by the operating system.
- `os.Signal` values are valid map keys in supported Go implementations. Platform support for a particular signal is
  ultimately determined by the target OS.
- Constructors copy mappings. Later caller mutation has no effect.
- `Stop` means stop accepting new source input and terminate internal goroutines. Unless a variant says otherwise, it
  does not guarantee delivery of events buffered in the OS or source.
- Panics in user callbacks are application faults. The callback variant defines an explicit recovery policy rather than
  letting a panic silently kill package coordination.

### Common validation

All constructors reject:

- a nil or empty mapping with `ErrEmptyMapping`;
- a nil signal key with `ErrInvalidSignal`;
- invalid buffer sizes or unsupported delivery-policy combinations with `ErrInvalidOption`.

A map cannot contain duplicate keys. If a convenience slice form is later added, its constructor must reject duplicate
signals rather than silently choosing the last value. Unmapped signals are ignored defensively, though production
registration includes only mapped signals. Signal validity is not fully portable to determine at runtime; the package
documents supported build targets and lets the underlying signal registration handle OS-specific values. Optional
platform allowlists should be implemented in platform-specific files only when the package intentionally supports a
restricted signal set.

All variants should expose only a map-based constructor initially. Options are justified only for buffer size,
backpressure, or callback panic handling; avoid options for behavior that has one sensible meaning.

### Common errors

```go
var (
	ErrEmptyMapping   = errors.New("signalmap: mapping is empty")
	ErrInvalidSignal  = errors.New("signalmap: signal is nil")
	ErrInvalidOption  = errors.New("signalmap: invalid option")
	ErrAlreadyStarted = errors.New("signalmap: listener already started")
	ErrNotStarted     = errors.New("signalmap: listener not started")
	ErrStopped        = errors.New("signalmap: listener is stopping or stopped")
	ErrInvalidHandler = errors.New("signalmap: handler is nil")
)
```

`Start` returns `ErrAlreadyStarted` after the first successful transition from `New`, whether the current state is
`Running`, `Stopping`, or `Stopped`. `Stop` is idempotent after start: concurrent and repeated calls wait for the same
termination and return the same terminal result. `Stop` in `New` returns `ErrNotStarted` and does not consume the one
allowed start. Constructors wrap validation details with `%w` where useful. Runtime source failures are returned by
`Wait` or `Stop`, depending on the variant; ordinary cancellation returns `nil` unless the API explicitly returns a
context error.

### Internal source abstraction

Production code may use `signal.Notify` or `signal.NotifyContext`, but tests should use a package-private source:

```go
type signalSource interface {
	Subscribe(signals []os.Signal) (<-chan os.Signal, func(), error)
}
```

The returned cleanup function is idempotent, unregisters delivery, and releases source resources. A production
`notifySource` owns the writable signal channel, calls `signal.Notify`, and calls `signal.Stop` during cleanup. Internal
coordination invokes cleanup exactly once before declaring the listener stopped. Tests inject a fake source whose input
is controlled without process-global signal state.

The production source buffer should be at least one and normally match the configured event capacity. A full
`os/signal` channel can cause notification loss; no variant claims stronger delivery than its source and policy permit.

## Variant 1: single-consumer receive-only channel

### Description and intended use

One caller starts a listener and receives mapped events from one channel. This fits command servers, daemons, and
applications with a central `select` loop. Multiple goroutines may receive from the channel, but they compete for events;
this is work sharing, not broadcast.

### Public API sketch

```go
type Listener[T any] struct { /* unexported */ }

func New[T any](mapping map[os.Signal]T, opts ...Option) (*Listener[T], error)

func (l *Listener[T]) Start(ctx context.Context) (<-chan T, error)
func (l *Listener[T]) Stop() error
func (l *Listener[T]) Wait() error
```

`Start` requires a non-nil context. The returned channel is stable and is returned only on the successful first call.
The caller must not close it. `Wait` blocks only after start; in `New` it returns `ErrNotStarted`.

Options select a non-negative event buffer size and one delivery policy:

```go
type OverflowPolicy uint8

const (
	Block OverflowPolicy = iota
	DropNewest
	DropOldest
)
```

`Block` is the default. Buffer size zero is allowed for `Block`; drop policies require a positive buffer.

### Components and concurrency model

- A mutex protects lifecycle state, the terminal error, and publication of `cancel`, `done`, and the event channel.
- `Start` performs validation already guaranteed by construction, locks, checks `New`, publishes all running fields,
  changes state to `Running`, and starts exactly one coordinator goroutine before unlocking.
- The coordinator is the sole sender and sole closer of the event channel. It maps source values and owns overflow
  operations, so no send/close race exists.
- `Stop` copies `cancel` and `done` under the mutex, unlocks, calls cancellation, then waits on `done`. It never waits
  while holding the mutex.
- The coordinator calls source cleanup, closes the event channel, records the terminal result/state under the mutex, and
  finally closes `done`. Deferred cleanup handles every exit path.
- Concurrent `Start` calls serialize on the mutex; exactly one succeeds. Concurrent `Stop` and `Wait` calls observe the
  same `done` and terminal error.

### Lifecycle

| State | `Start` | `Stop` | `Wait` |
|---|---|---|---|
| `New` | transition to `Running` | `ErrNotStarted` | `ErrNotStarted` |
| `Running` | `ErrAlreadyStarted` | cancel, wait for `Stopped` | wait |
| `Stopping` | `ErrAlreadyStarted` | wait | wait |
| `Stopped` | `ErrAlreadyStarted` | return terminal result | return terminal result |

Cancellation first transitions `Running` to `Stopping`. Only the coordinator transitions to `Stopped`. There is no reset
or restart path.

### Delivery, ordering, and backpressure

- Delivery is to one receiver. Each source value observed while running is mapped once and causes at most one send.
- Signals are handled in the order received from the source. The event channel preserves coordinator send order.
- Duplicate observed signals produce duplicate events. Unmapped signals are ignored.
- `Block` provides guaranteed delivery for signals the coordinator has received while it remains running, but a slow
  receiver backpressures the coordinator and increases upstream loss risk. The send uses `select` with cancellation, so
  shutdown cannot remain blocked.
- `DropNewest` drops the incoming event if the bounded event channel is full.
- `DropOldest` removes one queued event and enqueues the new one. Because only the coordinator accesses the send side and
  overflow operation, this is race-free; a concurrent receiver can make removal unnecessary, in which case enqueue
  proceeds normally.
- Drop counts should be observable through an optional caller-supplied metrics hook or an atomic `Dropped() uint64`
  accessor. They are not returned as synchronous errors because `Start` is asynchronous.
- On shutdown, no new events are accepted. Already buffered events remain readable before the coordinator closes the
  channel. A send currently waiting under `Block` may be abandoned when cancellation wins.

### Cancellation and cleanup

Either the `Start` context ending or `Stop` being called cancels the internal context. Cleanup calls `signal.Stop` through
the source before the event channel and `done` are closed. `Wait` returns after all package goroutines have exited.

### Testing strategy

- Fake-source tests for mapping, unmapped input, duplicate observations, ordering, and closed output.
- Table tests for nil/empty mappings, nil keys, options, map copying, and multiple signals sharing an event.
- Barrier-controlled concurrent `Start` tests proving one success and `ErrAlreadyStarted` for all others.
- Concurrent `Stop`/`Wait` tests, including cancellation during a blocked send, under `go test -race`.
- Buffer-policy tests for blocking, drop-newest, drop-oldest, and drop accounting.
- A small Unix-only subprocess integration test sends a safe signal to itself and verifies cleanup. Avoid process-global
  signal tests in parallel.

### Pros

- Minimal, idiomatic API that integrates with `select`.
- Explicit channel ownership and straightforward ordering.
- One internal goroutine and simple leak-free shutdown.
- Caller controls event processing and panic policy.

### Cons

- No broadcast semantics.
- Blocking delivery can amplify source loss; dropping policies weaken guarantees.
- A channel does not directly report per-event delivery failures.

### Risks

- Users may incorrectly assume OS signals are queued without coalescing.
- Users may start multiple receivers expecting each to see every event.
- An unconsumed channel with `Block` stalls delivery, though cancellation still terminates it.

### Usage limitations

- Not appropriate when every subscriber must receive every event.
- Not a durable queue and provides no replay.
- Exact signal counts cannot be guaranteed.

## Variant 2: callback/handler dispatcher

### Description and intended use

Users register handlers, and every handler snapshot registered at dispatch time receives each mapped event. This is useful
when independent components react to the same lifecycle event. It is the strongest pub/sub option here.

### Public API sketch

```go
type HandlerID uint64
type Handler[T any] func(context.Context, T)

type Dispatcher[T any] struct { /* unexported */ }

func NewDispatcher[T any](
	mapping map[os.Signal]T,
	opts ...DispatcherOption,
) (*Dispatcher[T], error)

func (d *Dispatcher[T]) Subscribe(h Handler[T]) (HandlerID, error)
func (d *Dispatcher[T]) Unsubscribe(id HandlerID) bool
func (d *Dispatcher[T]) Start(ctx context.Context) error
func (d *Dispatcher[T]) Stop() error
func (d *Dispatcher[T]) Wait() error
```

Nil handlers are rejected with `ErrInvalidHandler`. `Subscribe` is valid in `New` and `Running`, but not once stopping
begins (`ErrStopped`). `Unsubscribe` is valid in `New` and `Running`; it returns whether the ID existed. A successful
unsubscribe guarantees no future dispatch snapshot includes that handler, but an invocation already in progress may
finish.

The default dispatch policy is sequential. An explicit bounded-concurrent option may set worker count and per-subscriber
queue capacity. Unbounded “goroutine per event” dispatch is not offered.

### Components and concurrency model

- A state mutex protects lifecycle fields and terminal result.
- A separate subscriber mutex protects an immutable-by-dispatch handler map. Dispatch copies handler entries under a
  read lock, releases it, then invokes or enqueues callbacks. User code is never called while a package lock is held.
- IDs are monotonically allocated under the subscriber mutex; zero is invalid.
- One coordinator owns source consumption. Sequential mode invokes snapshot handlers in ascending ID order.
- Bounded-concurrent mode gives each subscriber one bounded mailbox and one worker goroutine, preserving ordering per
  subscriber while isolating slow subscribers. The worker set is finite and joined during shutdown.
- `Unsubscribe` removes the handler, closes its mailbox only through coordinator-owned control, and waits for its worker
  only if the contract is configured as synchronous. The recommended contract is non-blocking removal: existing work can
  complete, and global `Wait` joins retired workers.
- Channel closing and subscriber retirement occur in the coordinator, driven by control messages. This single ownership
  prevents send/close races during concurrent subscribe, unsubscribe, and stop.

### Lifecycle

Lifecycle is `New → Running → Stopping → Stopped`, with the same start-once and idempotent-stop rules as Variant 1.
Subscriptions may be changed in `New` or `Running`. Once `Stopping`, `Subscribe` returns `ErrStopped`, and
`Unsubscribe` returns false because all subscriptions are already being retired.

`Start` may allow zero handlers: events are ignored until a handler subscribes. This supports components that appear
dynamically and is not a configuration error.

### Delivery, ordering, and backpressure

Sequential mode:

- Every handler in the event's snapshot is called once, in ascending ID order.
- Events are processed in source order, and all callbacks for event N finish before event N+1 begins.
- A slow handler delays all handlers and source reads.
- Cancellation is passed to handlers, but Go cannot forcibly terminate a handler that ignores it. Therefore `Stop` and
  `Wait` cannot guarantee prompt return if user code blocks forever.

Bounded-concurrent mode:

- Every subscriber has its own worker and bounded queue; per-subscriber ordering is preserved.
- No total order exists across subscribers.
- Overflow policy is per subscriber: `Block`, `DropNewest`, or `DropOldest`. The recommended default for this mode is
  `DropNewest`, preventing one subscriber from blocking all dispatch.
- Dropped deliveries are counted per subscriber and may invoke a non-blocking metrics hook.
- Shutdown stops intake, closes subscriber mailboxes through their owner, and by default drains already queued events.
  An option may discard queued events for faster shutdown; this choice must be fixed at construction.

Callbacks run with a listener-owned context canceled during stop. The default panic policy recovers a handler panic,
reports it to an optional error hook, and continues other handlers. A repanic option is intentionally avoided because
panicking in an internal goroutine gives callers poor control; applications that require crash-on-panic can make their
error hook panic.

### Cancellation and cleanup

The coordinator unregisters the signal source before retiring workers. `Wait` joins the coordinator and all subscriber
workers. Sequential mode's documented limitation is that package cleanup depends on callbacks returning. Bounded mode
has the same limitation for callbacks already executing; queued callbacks are bounded and drain/discard behavior is
explicit.

### Error model

Configuration and lifecycle methods return sentinel errors. Asynchronous handler panics and source failures go to a
construction-time `func(error)` hook and are retained as the terminal `Wait` result where applicable. Overflow is
metrics/status, not a method error. Error hooks must be documented as non-blocking and panic-free; invocation can be
serialized by the coordinator.

### Testing strategy

- Fake-source broadcast tests with deterministic handler snapshots and ordering.
- Subscribe/unsubscribe races with barriers around snapshot creation.
- Slow, blocking, and panicking handlers; overflow behavior for each policy.
- Prove bounded goroutine count and that all workers join after callbacks return.
- Concurrent lifecycle method tests and race-detector runs.
- Integration signal test in an isolated Unix subprocess.

### Pros

- Broadcasts each observed event to all relevant subscribers.
- Dynamic subscription supports modular applications.
- Per-subscriber workers can isolate slow consumers while preserving local order.

### Cons

- Largest implementation and API surface.
- Callback lifetime and panic behavior require careful documentation.
- Prompt shutdown cannot be guaranteed when arbitrary callbacks do not return.
- Bounded isolation creates per-subscriber goroutines and queues.

### Risks

- Reentrant callbacks may call `Subscribe`, `Unsubscribe`, or `Stop`; locks must never be held during callback execution.
- Blocking overflow can recreate head-of-line blocking.
- A large subscriber count increases memory and scheduling cost.

### Usage limitations

- Not suitable for untrusted handlers that may block indefinitely.
- No durable delivery, replay, or cross-subscriber global ordering.
- For high fan-out, delivery acknowledgements, or complex routing, use a dedicated event bus after the signal adapter.

## Variant 3: context-oriented subscriptions

### Description and intended use

Mapped events cancel derived contexts. It is ideal when signals represent lifecycle transitions such as shutdown or
reload cancellation. It uses `signal.NotifyContext` in production where practical, while retaining a fakeable source
boundary.

Because plain context cancellation carries only one cause, this design creates one subscription per selected event and
uses `context.WithCancelCause` so callers can inspect the mapped value through a typed wrapper.

### Public API sketch

```go
type EventCause[T any] struct {
	Event T
}

func (e EventCause[T]) Error() string

type ContextListener[T any] struct { /* unexported */ }

func NewContextListener[T any](
	mapping map[os.Signal]T,
) (*ContextListener[T], error)

func (l *ContextListener[T]) Start(ctx context.Context) error
func (l *ContextListener[T]) Context(parent context.Context) (context.Context, error)
func (l *ContextListener[T]) Stop() error
func (l *ContextListener[T]) Wait() error
```

`Context` is valid in `New` and `Running`. It returns a child canceled by the first mapped event observed after its
registration, by its parent, or by listener shutdown. Calling it once stopping begins returns `ErrStopped`.
`EventCause[T]` can be recovered with `errors.As(context.Cause(ctx), &cause)`.

For the simplest shutdown-only case, an alternative narrow constructor may directly wrap `signal.NotifyContext`; it
should not be mixed into the generic multi-event API.

### Components and concurrency model

- Lifecycle fields use the same mutex and state machine as Variant 1.
- Active subscriptions are stored under a subscription mutex. Each entry owns a `context.CancelCauseFunc`.
- The coordinator consumes signals and cancels all active subscription contexts with the mapped `EventCause[T]`.
  Cancellation is broadcast and idempotent.
- A canceled subscription is removed by a small watcher goroutine waiting on its context, or by coordinator control.
  To avoid one watcher per subscription, the recommended implementation uses coordinator control messages: registration
  includes the parent context, and a watcher is still required to notice arbitrary parent cancellation. Therefore this
  variant accepts one bounded-lifetime watcher per active subscription and joins it during listener shutdown.
- Each watcher selects on its context and listener `done`, sends a removal request with a cancellation-aware select, and
  exits. No watcher can remain after `Wait`.
- The coordinator owns cancellation of listener-driven subscriptions; mutexes protect registration/removal so
  cancellation never races map mutation.

### Lifecycle

The listener follows `New → Running → Stopping → Stopped`; `Start` succeeds once. Contexts may be created before start so
applications can wire dependencies first. They become active when the listener starts. If the parent is already done,
the returned context is immediately canceled and is not retained.

During `Stopping`, new contexts are rejected. Existing contexts are canceled with `ErrStopped` unless already canceled by
an event or parent. After `Stopped`, `Stop` and `Wait` return the terminal listener result.

### Delivery, ordering, and backpressure

- Each active subscription is canceled on the first mapped event it observes; later signals cannot change its cause.
- Delivery is broadcast to all active contexts through cancellation.
- Cancellation is non-blocking and has no event queue, so slow consumers cannot backpressure the signal loop.
- There is no stream and no duplicate-event representation. Ordering is meaningful only for the first event: the first
  coordinator-observed event wins for each context.
- A subscriber registered concurrently with dispatch either appears in the locked snapshot and receives that event or
  becomes active immediately afterward; no stronger linearization promise is made beyond the lock boundary.
- Unmapped signals are ignored.

Production may use one `signal.NotifyContext` for the union of mapped signals, but it does not reveal which signal caused
cancellation. Therefore generic remapping still requires observing the actual `os.Signal`, making `signal.Notify` behind
the source abstraction the practical default. `signal.NotifyContext` is best reserved for the shutdown-only narrow API.

### Cancellation and cleanup

Parent cancellation only affects that subscription. Start-context cancellation or `Stop` stops the listener, unregisters
OS notifications, cancels remaining child contexts, joins watchers, and closes `done`. The stop function returned
internally by `signal.NotifyContext`, when used, must always be called to restore default signal behavior and release
resources.

### Error model

Constructor and lifecycle errors use the common sentinels. Event values are communicated as typed cancellation causes.
Listener shutdown uses `ErrStopped`; parent cancellation preserves the parent's cause. Source failure becomes the cause
for still-active children and the terminal `Wait` result.

### Testing strategy

- Fake-source tests for first-event-wins, typed cause extraction, broadcast, parent cancellation, and pre-start contexts.
- Concurrent registration/dispatch tests around the documented linearization boundary.
- Tests proving watcher count returns to zero and `Wait` joins all internal goroutines.
- Start-once, concurrent stop, source cleanup, and race-detector tests.
- A separate integration test for the narrow `signal.NotifyContext` wrapper, isolated in a subprocess.

### Pros

- Natural composition with Go cancellation and request lifetimes.
- Broadcast is non-blocking, with no event-buffer policy.
- First-event semantics are explicit through cancellation causes.

### Cons

- Context is not an event stream; it loses subsequent and duplicate events.
- Typed event extraction from `error` causes is less direct than receiving `T`.
- Parent cancellation tracking can require one watcher per active context.

### Risks

- Users may misuse context values or expect multiple events.
- Failure to call the underlying notify stop function would leak registrations and alter later signal behavior.
- Very high subscription churn creates goroutine scheduling overhead.

### Usage limitations

- Appropriate only when the first event ends or invalidates work.
- Not suitable for repeated reload/rotate events.
- Not the best choice for large, long-lived pub/sub populations.

## Comparison

| Dimension | Single-consumer channel | Callback dispatcher | Context-oriented |
|---|---|---|---|
| Primary semantic | Work/event stream to one consumer | Broadcast stream | Broadcast first-event cancellation |
| API complexity | Low | High | Medium |
| Ordering | Total send order | Total sequential or per-subscriber | First observed event only |
| Slow-consumer isolation | Buffer/drop policy | Per-subscriber workers/queues | Inherent; cancellation does not block |
| Dynamic subscribers | Competing receivers only | Yes | Yes, one-shot |
| Goroutines | One coordinator | Coordinator plus optional workers | Coordinator plus active-context watchers |
| Best use | Central application event loop | Concurrent modular pub/sub | Shutdown/cancellation |
| Main limitation | No broadcast | Handler behavior affects shutdown | No repeated events |

## Recommendation

Use the single-consumer channel architecture by default. It offers the clearest ownership contract, the smallest exported
surface, deterministic ordering, and the easiest race-free implementation. Choose `Block` for applications that consume
promptly and value delivery, or a bounded `DropNewest` policy when remaining responsive is more important than observing
every notification.

Do not use the default when all components must independently observe an event. Use the callback dispatcher for
highly concurrent pub/sub within the scope of these variants, with bounded per-subscriber queues and `DropNewest` as the
safe isolation default. For genuinely high-scale or durable pub/sub, adapt signals into an established event bus rather
than expanding this package.

For the simplest shutdown case, use the context-oriented narrow API around `signal.NotifyContext`. It directly expresses
“cancel work when this signal arrives” and avoids inventing an event stream. Do not use it for repeatable events such as
log reload or configuration refresh.

## Implementation and acceptance guidance

- Keep exported interfaces absent unless callers need substitution. Tests can inject an unexported source through an
  unexported constructor.
- Keep lifecycle state and terminal result under one mutex. Never block, invoke user code, or call cleanup while holding
  it.
- Give every channel exactly one closing owner. Call `signal.Stop` or the `NotifyContext` stop function before publishing
  terminal completion.
- Copy mappings during construction and never expose the copy.
- Document channel direction, closure, overflow, event ordering, OS coalescing, and start-once behavior on exported
  identifiers.
- Run unit tests with `go test -race`. Keep real-signal tests Unix-gated, subprocess-isolated, non-parallel, and limited
  to signals that cannot terminate the test unexpectedly.
- Acceptance requires deterministic tests proving exactly one concurrent `Start` succeeds, concurrent `Stop` calls
  terminate, cancellation unblocks backpressured delivery, source cleanup runs once, and all package goroutines exit.

## Final checklist

- [x] Concurrency safety and shared-state ownership
- [x] Start-once lifecycle and concurrent method behavior
- [x] Cancellation and deterministic goroutine termination
- [x] Signal registration and cleanup
- [x] Mapping validation and defensive copying
- [x] Delivery, ordering, duplication, and shutdown semantics
- [x] Backpressure alternatives and slow-consumer behavior
- [x] Typed/sentinel error model
- [x] Deterministic and integration testing strategy
- [x] Unix/platform constraints
- [x] Idiomatic naming, context, channels, mutexes, and minimal API principles

## Execution metadata

- Execution mode: planning
- Model/effort filename: `codex-5.5-low.md`
- Phase timing: not recorded reliably by the execution environment
- Token usage by input/output/cache category: unavailable to the document generator
