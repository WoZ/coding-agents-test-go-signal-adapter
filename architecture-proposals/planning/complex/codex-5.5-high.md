# OS Signal-to-Event Adapter Architecture Proposals

## Executive summary

This document proposes four architectures for a Unix Go 1.26 package named
`signalmap`. The package translates configured `os.Signal` values into
caller-defined values without exposing `os/signal` registration channels or
cleanup mechanics.

The recommended default is **Variant 1, a single-consumer event channel**. It
has the smallest API, one clear channel owner, deterministic shutdown, and
composes naturally with the caller's existing event loop. Use **Variant 2**
when application-owned handlers are more convenient than channels, **Variant
3** when the adapter should run synchronously within an existing
`context.Context`-controlled component, and **Variant 4** when every event must
be fanned out to multiple independently paced subscribers.

The package can prevent package-level data races and leaks, but it cannot make
OS signal delivery lossless. `os/signal` sends to the registered channel
without waiting; a full raw-signal buffer can therefore lose notifications.
Unix may also coalesce repeated standard signals before Go observes them. No
variant promises one event for every signal sent by the operating system.

## Assumptions and common contracts

- The target is Unix and Go 1.26. Platform-specific signal constants used by
  callers must exist on the target selected at build time.
- A mapping value has type `E any`. Multiple signals may map to equal or
  identical events. The package never compares `E`.
- The constructor shallow-copies the caller's mapping. It therefore protects
  the association between a signal and a value, but cannot protect mutable data
  referenced by `E`; callers must not mutate such data concurrently.
- Only configured signals are registered. An unmapped signal cannot reach the
  mapper through its private registration channel.
- Delivery guarantees begin only after successful start and end when stopping
  begins. They do not imply that the operating system or `os/signal` observed
  every signal.
- The zero value is not usable. A constructor is required because mapping
  validation and defensive copying are required.
- `Start` is a lifetime operation. Exactly one call can succeed for an object.
  Stopping never resets it to a startable state.

### Common validation

A constructor performs all configuration validation before allocating runtime
resources:

- A nil or empty map is rejected with `ErrInvalidMapping`.
- A nil `os.Signal` key is rejected. A map already guarantees unique keys.
- A slice-based convenience constructor, if added later, must reject duplicate
  signal entries rather than silently choosing the last one.
- Values of `E`, including zero values and typed nil pointers, are accepted.
- Multiple signals may map to the same event.
- Buffer sizes must be non-negative and bounded by a documented implementation
  maximum if options expose them.
- Drop policies must be members of the package's exported policy enum.

There is no portable runtime predicate for whether an arbitrary `os.Signal`
value is supported by the current kernel. The public API should consequently
prefer platform-defined values such as `syscall.SIGTERM`. Constants that do not
exist on a target fail at compile time. Tests using fabricated `os.Signal`
implementations can be rejected by an optional internal source validator, but
the production `os/signal` API itself reports no registration error. The
documentation must not claim stronger validation.

### Common signal source and cleanup

Production code hides `os/signal` behind an unexported, two-method dependency:

```go
type signalSource interface {
	Notify(chan<- os.Signal, ...os.Signal)
	Stop(chan<- os.Signal)
}
```

The production implementation delegates to `signal.Notify` and `signal.Stop`.
A fake implementation records registrations and injects signals
deterministically. The source is supplied through an unexported test
constructor, avoiding a public interface that exists only for tests.

The adapter creates a buffered raw-signal channel, calls `Notify` once, and
calls `Stop` exactly once during shutdown. It never closes the raw-signal
channel: the adapter cannot prove that `os/signal` has no in-flight sender until
cleanup has completed, and closing the channel is unnecessary. Only the
goroutine that sends mapped events closes an exported event channel.

Recommended common errors are deliberately small:

```go
var (
	ErrStarted        = errors.New("signalmap: already started")
	ErrNotStarted     = errors.New("signalmap: not started")
	ErrStopped        = errors.New("signalmap: stopped")
	ErrInvalidMapping = errors.New("signalmap: invalid mapping")
	ErrInvalidOption  = errors.New("signalmap: invalid option")
)
```

Configuration errors wrap the appropriate sentinel with `%w` and a useful
reason. `Stop` is idempotent after a successful start. A stop before start
returns `ErrNotStarted`. Every start after the first successful start returns
`ErrStarted`, including while stopping and after stopped.

## Variant 1: single-consumer event channel

### Description and intended use

One listener owns one signal registration and exposes one receive-only channel.
Each mapped event is consumed by exactly one receiver. This is the best fit for
a main event loop, a service supervisor, or an application that will perform
its own fan-out.

### Public API sketch

```go
package signalmap

type Listener[E any] struct {
	// unexported
}

func New[E any](mapping map[os.Signal]E, opts ...Option) (*Listener[E], error)

func (l *Listener[E]) Start() (<-chan E, error)
func (l *Listener[E]) Stop() error
func (l *Listener[E]) Done() <-chan struct{}
```

`Option` should initially expose only materially useful tuning, such as raw and
event buffer capacities. Defaults should be documented and useful without
options. Avoid an option for dependencies; the fake source belongs in an
unexported test constructor.

### Internal components and concurrency

- `mu sync.Mutex` protects lifecycle state and publication of runtime channels.
- Immutable copied mapping and sorted-or-stable signal slice are published by
  the constructor and never mutated.
- `raw chan os.Signal` receives from `os/signal`.
- `events chan E` has exactly one sender: the delivery goroutine.
- `stop chan struct{}` is closed once to request cancellation.
- `done chan struct{}` is closed by the delivery goroutine after cleanup and
  output closure.

The constructor creates the stable `done` channel returned by `Done`; it must
never be replaced. `Start` locks `mu`, verifies `New`, creates the remaining
runtime channels, registers the source, changes the state to `Running`,
launches the goroutine, and unlocks. This publishes a complete runtime state
before any concurrent `Stop` can act.

The delivery goroutine selects between `stop` and `raw`. For a raw signal it
looks up the immutable event and sends it with:

```go
select {
case events <- event:
case <-stop:
	return
}
```

The stop branch therefore interrupts a blocked event delivery. No mutex is held
while waiting on a channel or while waiting for shutdown.

### Lifecycle and cancellation

| State | `Start` | `Stop` | `Done` |
|---|---|---|---|
| `New` | Register and enter `Running` | `ErrNotStarted` | Returns open channel |
| `Running` | `ErrStarted` | First caller enters `Stopping` | Returns shared channel |
| `Stopping` | `ErrStarted` | Wait for shared `done`, then nil | Returns shared channel |
| `Stopped` | `ErrStarted` | nil | Already closed |

The first `Stop` transition closes `stop` under `mu` and marks that caller as
the cleanup owner. It then releases `mu`, calls source `Stop` exactly once, and
waits for `done`; no potentially blocking source operation occurs under the
state mutex. Concurrent stop callers only wait for the same `done`. The
delivery goroutine discards buffered raw signals once cancellation wins, closes
`events`, records `Stopped` under `mu`, and closes `done`. Shutdown does not
drain buffered mapped events; already buffered events remain readable before
the closed event channel is exhausted.

### Delivery, ordering, and backpressure

- Delivery is single-consumer, not broadcast.
- Mapped events observed by the delivery goroutine are sent in receive order.
- Duplicate observed signals produce duplicate events.
- Default delivery is blocking with a bounded event buffer. A slow receiver
  eventually blocks the mapper, allowing the raw buffer to fill and
  `os/signal` to drop later notifications.
- Cancellation interrupts a blocked send, so a slow or abandoned receiver
  cannot leak the goroutine.
- The package closes `events`; callers only receive from it and must never
  close it.

A drop policy can be added later, but should not be in the initial API unless
loss under application backpressure is an accepted product requirement.

### Testing

Use the fake source to test mapping, registration arguments, event order,
duplicate inputs, unmapped fake inputs, buffer saturation, stop during a
blocked send, channel closure, and exactly-once cleanup. Barrier-controlled
tests should race many `Start` and `Stop` calls and assert one successful start.
Run all concurrency tests with `go test -race`; use goroutine-count or leak
checking only as a secondary assertion. A Unix integration test may send
`SIGUSR1` to the test process, must register first, must not run in parallel,
and must restore signal handling in cleanup.

### Pros

- Smallest public surface and clearest channel ownership.
- Preserves order without handler scheduling complexity.
- Stop can always unblock internal delivery.
- Easy to compose with `select`, timers, and application contexts.

### Cons

- Multiple receivers compete; it is not broadcast.
- A slow receiver creates global backpressure.
- Buffer selection is an operational concern.

### Risks and usage limitations

- A user who expects broadcast may accidentally split events among receivers.
- A full raw channel can lose signals without an observable delivery error.
- The shallow mapping copy does not make mutable event payloads immutable.
- This is unsuitable when multiple independent components must each observe
  every retained event.

## Variant 2: callback and handler registry

### Description and intended use

The listener invokes registered handlers for each mapped event. This variant
fits dependency-injected services whose internal architecture already uses
callbacks and where handlers are short, non-blocking operations.

### Public API sketch

```go
type Handler[E any] func(context.Context, E)

type HandlerListener[E any] struct {
	// unexported
}

type Registration struct {
	// unexported
}

func NewHandlerListener[E any](
	mapping map[os.Signal]E,
	opts ...Option,
) (*HandlerListener[E], error)

func (l *HandlerListener[E]) Handle(Handler[E]) (*Registration, error)
func (r *Registration) Close() error
func (l *HandlerListener[E]) Start(context.Context) error
func (l *HandlerListener[E]) Stop() error
func (l *HandlerListener[E]) Done() <-chan struct{}
```

Nil handlers are rejected with `ErrInvalidOption`. A registration is an opaque,
idempotently closable handle; functions are not used as map keys or identities.

### Internal components and concurrency

Lifecycle state uses the same mutex and state machine as Variant 1. A separate
`handlersMu sync.RWMutex` protects a map from monotonically allocated
registration IDs to handlers. `Handle` is valid in `New` and `Running`.
Closing a registration is valid in all states and is idempotent.

For each event, the single dispatcher copies the current handlers under a read
lock, releases the lock, and invokes the snapshot sequentially in registration
ID order. Registration and removal never wait for user code. Removal guarantees
only that later snapshots exclude the handler; an invocation already present
in a snapshot may still occur.

The context passed to each handler is the listener's run context. `Stop`
cancels a derived context before waiting. Handlers must return promptly when
that context is canceled. The package invokes no handler while holding an
internal lock and does not recover panics by default; silently converting a
programming panic into partial event delivery is surprising. Applications that
need recovery should wrap their handlers explicitly.

### Lifecycle and cancellation

States are `New`, `Running`, `Stopping`, and `Stopped`, with the same lifetime
start rule and errors as Variant 1. `Start(ctx)` registers signals and launches
the dispatcher; cancellation of `ctx` is equivalent to calling `Stop`.
Whichever path initiates shutdown performs source cleanup through `sync.Once`.
`Stop` waits until the current handler snapshot has returned and the dispatcher
has closed `Done`.

### Delivery, ordering, and backpressure

- Every handler in the event's snapshot receives the event once.
- Events and handlers are processed serially, preserving observed signal order
  for every handler.
- A slow handler delays all later handlers and events.
- There is no package-level event drop after the dispatcher receives a signal,
  but raw signal delivery can still overflow while a handler runs.
- There are no handler-owned channels for the package to close.

Do not spawn an unbounded goroutine per callback. An optional concurrent worker
model would weaken ordering, complicate shutdown, and permit blocked user code
to consume workers indefinitely; it should be a separate type rather than an
option that silently changes this contract.

### Testing

In addition to common lifecycle tests, verify deterministic handler order,
registration during delivery, removal during delivery, snapshot semantics,
nil-handler rejection, context cancellation, handler-induced backpressure, and
panic propagation in a subprocess or dedicated goroutine. A blocking test
handler must be released during cleanup so the test itself does not leak.

### Pros

- Natural integration with service methods and dependency injection.
- Dynamic registration without exposing channels.
- Serial invocation gives a simple ordering contract.

### Cons

- Package shutdown depends on user handlers returning.
- One slow handler delays every handler.
- Handler panic policy becomes part of the application contract.

### Risks and usage limitations

- Go cannot forcibly cancel arbitrary user code. A handler that ignores
  cancellation and never returns prevents `Stop` from completing. This is a
  documented caller contract, not a condition the package can repair safely.
- This variant is inappropriate for untrusted handlers or long-running work.
- It is not performance isolation between listeners.

## Variant 3: context-oriented synchronous runner

### Description and intended use

`Run` owns the calling goroutine and remains active until its context is
canceled, `Stop` is called, the consumer returns an error, or signal processing
fails. This fits components already managed by an `errgroup`, service runner,
or structured-concurrency lifecycle. It has no hidden long-lived goroutine for
event dispatch.

`signal.NotifyContext` alone is not sufficient for general event remapping: it
records cancellation but not which of several signals caused it, and a context
transitions only once. This design therefore uses `signal.Notify` through the
common source to retain signal identity, while using `context.Context` as the
primary lifetime and cancellation contract.

### Public API sketch

```go
type Consumer[E any] func(context.Context, E) error

type Runner[E any] struct {
	// unexported
}

func NewRunner[E any](
	mapping map[os.Signal]E,
	consume Consumer[E],
	opts ...Option,
) (*Runner[E], error)

func (r *Runner[E]) Run(context.Context) error
func (r *Runner[E]) Stop() error
func (r *Runner[E]) Done() <-chan struct{}
```

`Run(nil)` and a nil consumer are rejected. Context is the first parameter, is
not stored before `Run`, and is never accepted as an optional value.

### Internal components and concurrency

The goroutine calling `Run` is the sole owner of raw event reception and
consumer invocation. A mutex protects only the lifecycle transition and stop
channel publication. `Stop` closes the stop channel and waits on `Done`; it
does not perform consumer work.

Under the state mutex, `Run` verifies `New`, creates its runtime channels,
registers the source, and only then publishes `Running`. It releases the mutex
before using a select over `ctx.Done`, the private stop channel, and raw
signals. This ordering prevents a concurrent `Stop` from cleaning up before
registration completes. Source cleanup and `Done` closure occur in deferred
functions in `Run`, guarded by `sync.Once` so an explicit concurrent `Stop`
cannot duplicate cleanup.

### Lifecycle and cancellation

The states and repeated-start errors match Variant 1, treating `Run` as the
start operation. `Run` returns:

- `nil` after an explicit `Stop`;
- `context.Cause(ctx)` after context cancellation, falling back to `ctx.Err()`;
- the consumer's error unchanged or wrapped only with stable operation context.

The consumer receives a child context canceled by either parent cancellation
or `Stop`. As in Variant 2, a consumer that never returns can delay shutdown.
Concurrent calls to `Run` result in one runner and `ErrStarted` for all others.

### Delivery, ordering, and backpressure

- One consumer receives every mapped event observed by the runner.
- Invocation is sequential and preserves observed order.
- Consumer execution is direct and blocking; no internal queue exists beyond
  the bounded raw channel.
- Cancellation is checked between events and passed into the consumer.
- Shutdown discards signals remaining in the raw buffer.

### Testing

Run the method in a test goroutine with a fake source and explicit barriers.
Cover parent cancellation before registration, cancellation during consumer
work, explicit stop, consumer error, cleanup on all return paths, concurrent
`Run` calls, ordering, and a full raw buffer. Verify exact returned errors with
`errors.Is`.

### Pros

- Strong structured-concurrency fit and straightforward error propagation.
- No hidden dispatch goroutine; its lifetime is the `Run` call.
- Minimal channel ownership exposed to callers.

### Cons

- Requires dedicating a caller goroutine to `Run`.
- A slow consumer stalls all processing.
- It cannot use `signal.NotifyContext` directly without losing event identity.

### Risks and usage limitations

- The consumer must honor context cancellation and return.
- It is not a pub/sub API.
- Applications expecting `Run` to return nil on parent cancellation must
  explicitly ignore `context.Canceled`; the package should preserve the cause.

## Variant 4: isolated channel pub/sub broker

### Description and intended use

A broker broadcasts each mapped event to all current subscribers. Each
subscriber owns a bounded package-created queue, and non-blocking overflow
policy prevents one slow subscriber from delaying others. This is the best
variant for highly concurrent pub/sub use.

### Public API sketch

```go
type OverflowPolicy uint8

const (
	DropNewest OverflowPolicy = iota
	DropOldest
)

type Broker[E any] struct {
	// unexported
}

type Subscription[E any] struct {
	// unexported
}

func NewBroker[E any](
	mapping map[os.Signal]E,
	opts ...BrokerOption,
) (*Broker[E], error)

func (b *Broker[E]) Subscribe(
	buffer int,
	policy OverflowPolicy,
) (*Subscription[E], error)
func (b *Broker[E]) Start() error
func (b *Broker[E]) Stop() error
func (b *Broker[E]) Done() <-chan struct{}

func (s *Subscription[E]) Events() <-chan E
func (s *Subscription[E]) Dropped() uint64
func (s *Subscription[E]) Close() error
```

Blocking delivery is intentionally absent: it conflicts with subscriber
isolation. A caller requiring lossless package-level delivery should use one
Variant 1 listener and build application-specific durable fan-out.

### Internal components and concurrency

- `stateMu` protects lifecycle state and runtime channel publication.
- `subsMu sync.RWMutex` protects the subscriber map and channel closure.
- One dispatcher goroutine is the only sender to subscriber channels.
- Each subscription has an atomic dropped-event counter and an idempotent close
  path tied to its broker.

`Subscribe` is valid in `New` and `Running`; it returns `ErrStopped` in
`Stopping` or `Stopped`. The dispatcher holds `subsMu.RLock` while performing
only non-blocking sends. `Subscription.Close` takes `subsMu.Lock`, removes the
subscription, and closes its channel while still holding the lock. Thus a close
cannot race a send. Broker shutdown first stops and joins the dispatcher, then
takes `subsMu.Lock` and closes all remaining subscription channels.

For `DropNewest`, a failed non-blocking send increments `Dropped`. For
`DropOldest`, the dispatcher attempts one non-blocking receive from the full
queue and retries one non-blocking send. Because the subscriber can receive
concurrently, the retry may still fail; in either loss case the counter is
incremented. These operations remain under the read lock, so unsubscribe cannot
close the channel concurrently.

### Lifecycle and cancellation

The broker uses `New`, `Running`, `Stopping`, and `Stopped` with the common
start/stop errors. Subscribers may be prepared before `Start`; their channels
remain open and empty. Starting with no subscribers is valid. During stopping,
new subscriptions fail, concurrent unsubscribe remains safe, and all extant
subscription channels eventually close. `Stop` does not drain the raw channel.

`Subscription.Close` is idempotent and causes its event channel to close after
any already-buffered values have been received. It does not stop the broker.
The broker, not the caller, owns and closes every subscription channel.

### Delivery, ordering, and backpressure

- Each subscriber present in the dispatch snapshot is offered each event once.
- A subscriber added concurrently may or may not receive the event currently
  being dispatched; after `Subscribe` returns, it receives later dispatches.
- After `Subscription.Close` returns, no new values can be sent to that
  subscription.
- Retained events preserve observed signal order per subscriber.
- Overflow is isolated per subscriber and visible through `Dropped`.
- `DropNewest` preserves older queued events. `DropOldest` favors recency.
- Duplicate observed signals produce duplicate delivery attempts.

### Testing

Test broadcast to many subscribers, pre-start subscriptions, concurrent
subscribe/unsubscribe during dispatch, closure guarantees, both overflow
policies, exact or monotonic dropped counts as specified, per-subscriber order,
slow-subscriber isolation, stop during high-volume dispatch, and channel close
without send-on-closed panic. Add stress tests under `go test -race -count`
with deterministic fake signals. Avoid asserting cross-subscriber wall-clock
receive order because goroutine scheduling is outside the contract.

### Pros

- True fan-out with explicit slow-subscriber isolation.
- Dynamic subscription is safe during operation.
- Loss is observable per subscriber.

### Cons

- Largest API and most complicated close synchronization.
- Bounded non-blocking queues necessarily permit loss.
- Per-subscriber buffers increase memory use.

### Risks and usage limitations

- Consumers may overlook the dropped counter and assume lossless broadcast.
- `DropOldest` involves concurrent receivers and therefore offers retained
  ordering, not a deterministic identity for the discarded event.
- This is not a durable queue and cannot guarantee delivery across crashes.
- Very large subscriber counts make each event an O(n) dispatch.

## Comparative analysis

| Variant | Recipients | Backpressure | Ordering | Shutdown dependency | Complexity | Best use |
|---|---|---|---|---|---|---|
| 1. Event channel | One consumer | Bounded, blocking | Observed order | Internal sends are cancellable | Low | Default and simple event loops |
| 2. Handlers | All handler snapshots | Slowest handler blocks all | Event and handler order | Handlers must return | Medium | Short trusted callbacks |
| 3. Context runner | One consumer | Consumer blocks runner | Observed order | Consumer must return | Low | `errgroup` or service runners |
| 4. Pub/sub broker | All current subscribers | Bounded drop per subscriber | Retained order per subscriber | Internally cancellable | High | Concurrent isolated fan-out |

## Recommendation

Implement Variant 1 first. It provides the best default because it has one
delivery goroutine, one output owner, explicit cancellation of blocked sends,
and a small API that callers can combine with normal Go `select` statements.
It also avoids making handler execution or multi-subscriber loss policy part of
the foundational package.

Do not use Variant 1 when several independent consumers must each observe the
same event; multiple receivers on its channel compete. For the simplest
context-managed service, Variant 3 is equally small and can be preferable when
the application already standardizes on blocking `Run(ctx) error` methods.
Use Variant 4 for highly concurrent pub/sub, provided bounded best-effort
delivery and explicit loss metrics are acceptable. Variant 2 is appropriate
only when callback convenience outweighs the shutdown and backpressure risks
of running application code in the dispatch path.

## Cross-variant implementation and test checklist

- **Concurrency safety:** immutable mapping; named mutex ownership; no channel
  close concurrent with send; no lock held across blocking user operations.
- **Lifecycle:** exactly one successful start; explicit four-state machine;
  safe concurrent start and stop; no reset.
- **Cancellation:** stop signal interrupts internal blocking operations;
  context-aware variants propagate cancellation to user code.
- **Signal cleanup:** every successful registration has exactly one matching
  `signal.Stop`; raw channels are never closed.
- **Mapping:** nil and empty mappings rejected; keys copied; duplicate slice
  entries rejected if such an API is introduced; shared events allowed.
- **Delivery:** recipient set, ordering, duplicates, buffering, shutdown drain,
  and unavoidable OS-level loss documented per variant.
- **Backpressure:** blocking only where explicit; pub/sub uses bounded
  non-blocking queues and observable drops.
- **Errors:** a small sentinel set with `%w` wrapping; returned context and
  consumer errors remain discoverable with `errors.Is`.
- **Testing:** fake source, lifecycle barriers, slow consumers, dynamic
  subscribers, cancellation, Unix integration, race detector, and leak paths.
- **Platform:** Unix build constraints for platform-specific integration tests;
  no false promise of portable runtime signal validation.
- **Style:** package name avoids stutter; exported names are short in package
  context; exported identifiers receive Go doc comments; interfaces stay
  internal and minimal; no reflection, `unsafe`, global registration state, or
  unstoppable hidden goroutines.

## Execution metadata

The requested execution mode was `planning`, which selected
`architecture-proposals/planning/simple` as the output directory. Exact
wall-clock phase timing and input/output/cache token counts were not exposed by
the execution environment, so no values are fabricated.
