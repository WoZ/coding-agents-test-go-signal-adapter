# Architecture proposals: Unix signal-to-event adapter

## Scope and assumptions

This document is based only on the task statement. No repository implementation or existing proposal was used.

The following assumptions apply to every variant:

- The target is Go 1.26 on Unix. Public configuration identifies signals with `syscall.Signal`; the package does not
  expose `os/signal` channels or registration mechanics.
- A configured signal maps to exactly one event. Duplicate signal mappings are rejected by the constructor. Multiple
  signals may map to the same event.
- Event type `E` may be any Go type. Delivery copies `E` according to normal Go assignment rules; referenced mutable
  data is not deep-copied.
- Configuration is validated and defensively copied at construction time, then treated as immutable.
- A successful `Start` consumes the object's one start opportunity permanently. After natural completion, cancellation,
  or `Stop`, another `Start` returns `ErrAlreadyStarted`.
- `Stop` is safe for concurrent and repeated calls. It requests shutdown; `Done` is closed only after signal
  registration has been removed and all package-owned goroutines have exited.
- A context already canceled before `Start` returns its context error and does not consume the start opportunity.
  Cancellation racing with an otherwise accepted start may produce a successful, immediately completed start.
- Catching `SIGKILL` or `SIGSTOP` is impossible. Unix signal delivery and Go's notification mechanism may coalesce
  repeated signals. None of these variants promises durable or exactly-once delivery.
- Other code in the same process must not independently reset or otherwise interfere with the same signal registrations
  while the adapter is active.

## Common contracts and lifecycle

A small shared configuration vocabulary keeps OS integration separate from the delivery model:

```go
package signalevent

import (
	"context"
	"errors"
	"os"
	"syscall"
)

var (
	ErrAlreadyStarted = errors.New("signal event adapter already started")
	ErrNotStarted     = errors.New("signal event adapter not started")
)

type Mapping[E any] struct {
	Signal syscall.Signal
	Event  E
}

type BufferPolicy uint8

const (
	Block BufferPolicy = iota
	DropNewest
)
```

Each concrete variant should expose this lifecycle shape:

```go
Start(context.Context) error
Stop() error
Done() <-chan struct{}
```

`Start` is non-blocking after registration and goroutine creation. The implementation uses an atomic compare-and-swap or
a mutex-protected state transition from `new` to `started`; there is intentionally no transition back to `new`. `Stop`
cancels an internal child context. Calling `Stop` before a successful `Start` returns `ErrNotStarted`; after a
successful start it is idempotent.

Internally, production code owns a buffered `chan os.Signal`, registers only configured signals, and always unregisters
it with `signal.Stop` on exit. It never closes that channel because the package does not own sends to it. The OS-facing
goroutine selects on both the signal channel and cancellation, and every potentially blocking downstream send also
selects on cancellation.

For tests, signal registration sits behind an unexported interface:

```go
type signalSource interface {
    Notify(chan<- os.Signal, ...os.Signal)
    Stop(chan<- os.Signal)
}
```

Production construction injects a thin `os/signal` implementation; package tests inject a deterministic fake. An
unexported test constructor avoids making the backend part of the public API. All variants should test duplicate
mappings, concurrent `Start`, `Start`/`Stop` races, cancellation at each blocking point, event mapping, output closure,
and cleanup under `go test -race`. Leak tests should repeatedly start and stop instances and verify fake-source
deregistration plus termination of `Done`.

## Variant 1: typed pull stream

### Description

The adapter exposes one receive-only event channel. A caller can consume it directly or compose it into an existing
`select` loop.

```go
type Stream[E any] struct {
    // unexported
}

type StreamOptions struct {
    EventBuffer  int
    SignalBuffer int
    FullBuffer   BufferPolicy
}

func NewStream[E any](
    mappings []Mapping[E],
    opts StreamOptions,
) (*Stream[E], error)

func (s *Stream[E]) Events() <-chan E
func (s *Stream[E]) Start(ctx context.Context) error
func (s *Stream[E]) Stop() error
func (s *Stream[E]) Done() <-chan struct{}
func (s *Stream[E]) Dropped() uint64
```

The output channel exists from construction and is closed exactly once during shutdown. One adapter goroutine receives a
signal, performs a lookup, and sends the mapped event. With `Block`, backpressure propagates to the signal receiver.
With `DropNewest`, a non-blocking send drops an event when the output buffer is full and increments an atomic counter.

`EventBuffer` and `SignalBuffer` must be non-negative and should have conservative documented defaults. A zero event
buffer is useful for synchronization but makes the caller directly responsible for timely consumption.

### Pros

- Small, idiomatic API that works naturally with contexts and `select`.
- No callback contract and only one package-owned goroutine.
- Preserves event order as observed by the adapter.
- Backpressure behavior is explicit and easily tested.
- The caller controls where and how event handling executes.

### Cons

- Exactly one logical consumer receives each event; competing readers divide work rather than broadcast it.
- A blocked or absent consumer either stalls reception or causes drops, depending on policy.
- The caller must run a receive loop and handle channel closure.
- A drop counter reports loss but does not identify which event was lost.

### Risks

- Users may incorrectly treat several readers as subscribers and expect each to see every event.
- Blocking delivery can amplify a slow consumer into missed/coalesced OS notifications.
- Mutable event values can race if the producer's referenced data is modified after construction.
- Very large buffers defer overload rather than solving it and increase memory use.

### Usage limitations

- Use only when one component owns the stream or when competing-consumer semantics are intentional.
- Do not use for per-subscriber broadcast.
- `Block` requires a consumer that continues receiving until cancellation; `DropNewest` requires loss-tolerant events.
- This is not suitable where every occurrence must be durably recorded.

## Variant 2: serialized handler

### Description

The adapter invokes one user-provided handler synchronously from its receive loop. It is the smallest push-oriented
design and makes serialized processing explicit.

```go
type Handler[E any] interface {
    HandleSignalEvent(context.Context, E) error
}

type HandlerFunc[E any] func (context.Context, E) error

func (f HandlerFunc[E]) HandleSignalEvent(ctx context.Context, event E) error {
    return f(ctx, event)
}

type HandlerOptions struct {
    SignalBuffer int
    OnError      func (error)
}

func NewHandlerAdapter[E any](
    mappings []Mapping[E],
    handler Handler[E],
    opts HandlerOptions,
) (*HandlerAdapter[E], error)
```

The lifecycle methods are the common `Start`, `Stop`, and `Done` methods. The handler runs at most once at a time and
receives the adapter's internal cancellation context. The default error policy records the first handler error and stops
the adapter; an optional error observer may report it. A `Wait() error` method can wait for `Done` and return that
terminal error without mixing operational errors into the event contract.

The constructor rejects a nil handler and nil error observer when one is required by a selected policy. The package
documents that a panic from the handler is recovered at the package boundary, converted to a terminal error with a stack
trace, and causes orderly shutdown. This prevents one bad event from bypassing registration cleanup; it does not claim
the process is safe to continue after arbitrary panic-causing corruption.

### Pros

- Minimal integration code for callers with one action to perform.
- Strictly serialized calls avoid handler reentrancy.
- No exposed event channel or consumer-loop plumbing.
- Terminal handler errors have a direct reporting path.
- Ordering as observed by the adapter is preserved.

### Cons

- Handler latency directly delays reception of later signals.
- The package calls user code from its sole processing goroutine.
- It is less composable than a channel and unsuitable for multiple independent listeners.
- Error and panic policy adds API decisions absent from the pull model.

### Risks

- A handler that ignores cancellation and never returns prevents completed shutdown; no Go API can forcibly terminate
  arbitrary user code safely.
- Reentrant calls from the handler into methods that wait for `Done` can deadlock.
- Slow logging or network I/O in the handler can cause signal coalescing or loss upstream.
- An `OnError` callback can create the same blocking or panic hazards unless constrained.

### Usage limitations

- The handler must return promptly and must honor context cancellation for blocking work. This is part of the API
  contract required for leak-free shutdown.
- The handler must not call a blocking `Wait` on its own adapter.
- Use only for short, bounded, serialized work. Long-running work should be handed to an independently managed
  component.
- This design provides neither parallel processing nor broadcast.

## Variant 3: managed pub/sub broker

### Description

The adapter owns a broker that broadcasts each mapped event to a dynamic set of subscriptions. Each subscriber has a
bounded mailbox and a dedicated forwarding goroutine, isolating fast subscribers from slow ones.

```go
type SlowSubscriberPolicy uint8

const (
    DropForSubscriber SlowSubscriberPolicy = iota
    DisconnectSubscriber
    BlockBroker
)

type Subscription[E any] interface {
    Events() <-chan E
    Dropped() uint64
    Unsubscribe()
}

type BrokerOptions struct {
    SignalBuffer    int
    SubscriberQueue int
    SlowSubscriber SlowSubscriberPolicy
}

func NewBroker[E any](
    mappings []Mapping[E],
    opts BrokerOptions,
) (*Broker[E], error)

func (b *Broker[E]) Subscribe() (Subscription[E], error)
func (b *Broker[E]) Start(context.Context) error
func (b *Broker[E]) Stop() error
func (b *Broker[E]) Done() <-chan struct{}
```

Broker state is owned by one command-loop goroutine after start. Subscribe, unsubscribe, publish, and shutdown requests
enter that loop through typed internal command channels. `Subscribe` is accepted only while the broker is running;
before `Start` it returns `ErrNotStarted`, and after terminal shutdown it returns a stable `ErrClosed`. This rule
prevents inert pre-start subscriptions and avoids concurrently closing a mailbox while publishing to it.

Each subscription has an internal bounded mailbox and a public receive-only channel. Its forwarding goroutine selects
between mailbox input, subscription cancellation, and broker cancellation. Only that goroutine closes its public
channel. Unsubscribe is idempotent. Broker shutdown stops OS notifications, prevents new subscriptions, cancels all
forwarding goroutines, waits for them, then closes `Done`.

For `DropForSubscriber`, only the overloaded subscriber misses the event and its counter increments.
`DisconnectSubscriber` removes and closes that subscriber. `BlockBroker` preserves broker-observed delivery while
subscribers remain connected, but every mailbox send must also select on cancellation so shutdown cannot deadlock.

### Pros

- True broadcast semantics for multiple independent listeners.
- Bounded per-subscriber memory and explicit slow-consumer policy.
- Slow subscribers can be isolated without delaying healthy subscribers.
- Dynamic subscription and unsubscription are race-safe.
- Command-loop ownership makes close/send ordering auditable and testable.

### Cons

- Highest implementation and testing complexity.
- At least one goroutine and two channels per active subscriber.
- Ordering is preserved per subscriber, but subscribers progress independently.
- Drop/disconnect metrics and lifecycle states expand the public contract.

### Risks

- A subtle close/send ordering bug can panic; ownership rules must remain centralized.
- `BlockBroker` lets one subscriber delay all others and should not be the default.
- Subscriber churn can create avoidable allocation and scheduler pressure.
- Broadcasting mutable event references can cause caller-side data races when subscribers mutate them.
- Loss policies can hide operational overload if metrics are not observed.

### Usage limitations

- Events must be immutable by convention, or the broker must accept a documented `Clone func(E) E` for per-subscriber
  copies.
- Every subscription must either be consumed or unsubscribed; broker cancellation remains the final cleanup mechanism.
- This is in-process, best-effort pub/sub, not a durable message broker.
- High subscriber counts require measured queue sizes and churn/load testing.

## Comparison

| Criterion                | Typed pull stream                | Serialized handler           | Managed pub/sub broker                          |
|--------------------------|----------------------------------|------------------------------|-------------------------------------------------|
| Delivery model           | One stream / competing consumers | One synchronous callback     | Broadcast to every active subscriber            |
| Package-owned goroutines | 1                                | 1                            | 1 broker + 1 per subscriber                     |
| Ordering                 | Preserved                        | Preserved                    | Preserved per subscriber                        |
| Slow-consumer handling   | Block or drop globally           | Handler blocks globally      | Per-subscriber drop/disconnect, or global block |
| API complexity           | Low                              | Lowest                       | High                                            |
| Composition              | Excellent with `select`          | Limited                      | Good for dynamic listeners                      |
| Error path               | Usually none; observe drops      | `Wait` returns handler error | Subscription state and metrics                  |
| Best fit                 | General single-owner integration | One short bounded action     | Concurrent in-process fan-out                   |

## Recommendation

The **typed pull stream** is the recommended default. Channels are the native Go abstraction for asynchronous values,
the caller retains control of scheduling and selection, shutdown is straightforward, and the implementation has the
smallest concurrency surface while remaining broadly composable. Default to a bounded buffer with documented capacity
and `Block`; select `DropNewest` only when the application explicitly accepts loss and monitors `Dropped`.

Do not use the pull stream when every one of several independent listeners must receive each event, or when the package
must own invocation and error handling. The **serialized handler** is best for the simplest case: one short,
cancellation-aware action and no need to compose the events with other channel inputs. The **managed pub/sub broker** is
best for highly concurrent in-process pub/sub, especially when subscribers have different throughput and need isolated
overload policies.

The broker should not be selected merely in anticipation of possible future listeners. Its lifecycle, per-subscriber
resource ownership, and overload semantics are materially more expensive to implement and validate.

## Verification strategy

All implementations should include:

- Table-driven constructor tests for empty mappings, duplicate signals, invalid buffers, and nil collaborators.
- A barrier-based test in which many goroutines call `Start`; exactly one succeeds and all others receive
  `ErrAlreadyStarted`.
- Tests proving cancellation, `Stop`, and natural context completion all deregister the fake source and eventually close
  `Done`.
- Tests that a completed object never starts again.
- Saturated-buffer tests for every backpressure policy.
- Ordering and remapping tests using synthetic signals through the fake source.
- Repeated start/stop and subscribe/unsubscribe stress tests under `go test -race`.
- A goroutine-leak test with bounded deadlines, while avoiding brittle assertions against the process-wide goroutine
  count.
- Broker tests for unsubscribe racing with publish and shutdown, and handler tests for errors, panic conversion, and
  cancellation-aware blocking.

## Execution metadata

The work was performed in default execution mode. Exact phase timing and token/cache usage were not exposed by the
execution environment, so no estimates are presented as measurements.
