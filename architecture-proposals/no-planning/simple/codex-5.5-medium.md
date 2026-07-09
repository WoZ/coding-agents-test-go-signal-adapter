# OS Signal Adapter Architecture Proposals

## Scope and common design rules

These proposals describe a Unix-oriented Go 1.26 package that translates operating-system signals into values chosen by
the caller. They deliberately hide signal registration, channel ownership, and `signal.Stop` handling behind a small
public API.

All variants use these lifecycle rules:

- A newly constructed object has a permanent `not started` state.
- `Start` atomically consumes that state before initializing signal handling. Every later call returns
  `ErrAlreadyStarted`, including calls made after cancellation or after a failed first initialization. There is no reset
  or restart API.
- `Cancel` is idempotent and concurrency-safe. Calling it before `Start` is a no-op and does not consume the one allowed
  start.
- Shutdown unregisters signal delivery, tells the dispatcher to stop, waits for package-owned goroutines, and closes
  package-owned output channels exactly once.
- Construction defensively copies the caller's mapping. Values are copied when delivered; callers remain responsible for
  synchronization if a value contains pointers, maps, slices, or other mutable shared state.
- Unmapped signals are ignored. Duplicate signal keys are naturally rejected by Go map semantics.

A representative shared contract is:

```go
package signalmap

import (
	"context"
	"errors"
	"os"
)

var ErrAlreadyStarted = errors.New("signal adapter already started")

// Mapping is copied during construction and is immutable thereafter.
type Mapping[E any] map[os.Signal]E
```

Internally, each design has a small signal-source interface. Production code implements it with `os/signal`; tests use a
fake source controlled without sending process signals. The interface is an implementation detail, not a public escape
hatch:

```go
type source interface {
  start([]os.Signal) (<-chan os.Signal, error)
  stop()
}
```

The production source owns a buffered signal channel, calls `signal.Notify` once, and calls `signal.Stop` during every
started shutdown path. State transitions, cancellation publication, and channel closure are guarded by a mutex plus
`sync.Once` values (or an equivalent atomic state machine). Package code never closes a channel it does not own.

Common tests should include concurrent `Start` calls, concurrent repeated `Cancel` calls, cancellation before and during
delivery, initialization failure, every mapped and unmapped signal, closure ordering, and race-detector stress tests.
Tests should also assert that all package-owned goroutines exit, using observable completion signals rather than sleeps.

## Variant 1: Single-consumer event stream

### Description

The adapter exposes one read-only event channel. `Start` returns the channel and starts one dispatcher goroutine. This
is
the smallest useful API and makes backpressure explicit.

```go
type Stream[E any] struct {
// unexported lifecycle, source, mapping, and synchronization fields
}

func NewStream[E any](mapping Mapping[E], capacity int) (*Stream[E], error)
func (s *Stream[E]) Start(ctx context.Context) (<-chan E, error)
func (s *Stream[E]) Cancel()
func (s *Stream[E]) Done() <-chan struct{}
```

The dispatcher selects between the source channel, context cancellation, and an internal cancellation channel. For each
mapped signal it sends with another cancellation-aware `select`, so a full event channel cannot prevent shutdown. It
performs cleanup in a deferred function, then closes the event and `Done` channels. The configured capacity is bounded;
zero means synchronous delivery.

`Start` should not hold a lifecycle mutex while invoking the source or launching the dispatcher. It first makes the
one-way state transition, then initializes through local values and publishes them safely.

### Assumptions

- Exactly one logical consumer needs each event. Multiple goroutines may receive, but they compete for events rather
  than each receiving a copy.
- Blocking delivery is desired while running; loss is not acceptable.
- The consumer continues receiving or explicitly cancels the stream.

### Pros

- Small, idiomatic API using channels and `context.Context`.
- Preserves order and provides straightforward backpressure.
- Only one long-lived package goroutine.
- Easy to test deterministically with a fake source.

### Cons

- It is a work-queue, not broadcast: multiple readers divide events.
- A slow consumer stops further event delivery.
- Buffer sizing is a caller concern.
- No per-listener filtering or delivery policy.

### Risks

- A cancellation-unaware send would leak the dispatcher when the output is full; every send must select on cancellation.
- Mutable data referenced by `E` can still race in caller code.
- OS signal delivery can coalesce before the adapter receives it; the architecture cannot promise one event per physical
  signal occurrence.

### Usage limitations

- Do not use when every listener must observe every event.
- Do not use an unbounded queue around the returned channel.
- Ordering is guaranteed only for values observed sequentially by the dispatcher, not for physical signal arrival at the
  operating-system level.

## Variant 2: Serialized handler

### Description

The caller supplies a handler invoked by the adapter's single dispatcher. This avoids exposing any channel and is useful
when signal handling is a direct command.

```go
type Handler[E any] func (context.Context, E) error

type Runner[E any] struct {
// unexported fields
}

func NewRunner[E any](mapping Mapping[E], handler Handler[E]) (*Runner[E], error)
func (r *Runner[E]) Start(ctx context.Context) error
func (r *Runner[E]) Cancel()
func (r *Runner[E]) Done() <-chan struct{}
func (r *Runner[E]) Err() error
```

Handlers run serially in signal-observation order. A handler error records the terminal error and initiates shutdown.
`Err` is safe during and after execution and returns the first terminal handler/source error. The dispatcher passes a
derived context that is canceled during shutdown. It does not create a goroutine per call: doing so would weaken
ordering and make it impossible to guarantee cleanup when arbitrary handlers block.

### Assumptions

- The handler is cooperative: it returns promptly when its context is canceled.
- Serialized processing is acceptable and desirable.
- Handler errors should terminate the adapter rather than be retried.

### Pros

- Minimal integration surface; the caller need not manage a channel.
- Naturally serialized side effects and deterministic ordering.
- Centralized error propagation.
- One package goroutine and no per-event allocation requirement beyond the chosen event type.

### Cons

- User code executes on the dispatch path.
- A slow handler delays all later events.
- The cooperative-handler contract is stronger than the channel variant's consumer contract.
- Reentrancy needs documentation: calling `Cancel` is safe, but waiting on `Done` from inside the handler would
  deadlock.

### Risks

- Go cannot forcibly terminate an arbitrary blocked handler. A handler that ignores cancellation can prevent shutdown;
  this is an API contract, not something the package can safely repair.
- Panic behavior must be defined. The recommended default is to recover at the dispatcher boundary, record a typed panic
  error, and shut down; alternatively, a package may deliberately let panics crash the process, but must test and
  document that policy.
- Handler-owned mutable state can race with other caller goroutines.

### Usage limitations

- Handlers must be bounded, cancellation-aware, and must not wait for the same runner's `Done` channel.
- This design is unsuitable for independent listeners or concurrent long-running work.
- Automatic retry and panic continuation are intentionally absent.

## Variant 3: Bounded pub/sub hub

### Description

The adapter broadcasts each mapped event to registered subscriptions. Each subscriber owns a bounded channel and chooses
an explicit overflow policy.

```go
type OverflowPolicy uint8

const (
    Block OverflowPolicy = iota
    DropNewest
    Disconnect
)

type Subscription[E any] struct {
    C <-chan E
    // unexported identity and hub reference
}

func (s *Subscription[E]) Close()

type Hub[E any] struct {
// unexported fields
}

func NewHub[E any](mapping Mapping[E]) (*Hub[E], error)
func (h *Hub[E]) Subscribe(capacity int, policy OverflowPolicy) (*Subscription[E], error)
func (h *Hub[E]) Start(ctx context.Context) error
func (h *Hub[E]) Cancel()
func (h *Hub[E]) Done() <-chan struct{}
```

Subscription changes are serialized through the hub's dispatcher command channel, or protected by a mutex while the
dispatcher takes a snapshot. No channel send occurs while holding the subscription mutex. `Subscription.Close` is
idempotent and waits until the dispatcher can no longer send before closing its channel, preventing send/close races.

With `Block`, delivery selects on subscriber readiness, subscription closure, and hub cancellation. `DropNewest` uses a
non-blocking send and increments an observable per-subscription drop counter. `Disconnect` removes and closes a
subscriber whose buffer is full. Invalid capacities or policies fail during subscription. On hub shutdown, all active
subscription channels are closed after signal delivery is stopped.

### Assumptions

- Every active subscriber normally needs each mapped event.
- Bounded buffers and an explicit slow-subscriber policy are acceptable.
- Subscription membership may change concurrently with signal delivery.

### Pros

- Supports independent listeners and concurrent subscribe/unsubscribe operations.
- Isolates slow listeners when `DropNewest` or `Disconnect` is selected.
- Bounded memory use with explicit overload semantics.
- Testable delivery, overflow, unsubscribe, and shutdown ordering.

### Cons

- Considerably more state and synchronization than a single stream.
- Broadcast cost is linear in the number of subscribers.
- `Block` allows one subscriber to delay all subscribers.
- Lossy policies require metrics or counters to make loss visible.

### Risks

- Closing subscription channels outside the ownership protocol can cause send/close panics.
- Snapshotting subscribers without a clear removal handshake can deliver after `Close` returns.
- Fairness across subscribers is not guaranteed by Go `select`; a fixed iteration order can systematically favor early
  subscribers, so the implementation should rotate the starting index if fairness matters.
- Large subscriber counts amplify per-signal CPU cost and retained buffer memory.

### Usage limitations

- This is an in-process notification mechanism, not a durable event bus.
- It provides per-subscriber FIFO ordering but no persistence, replay, acknowledgement, or exactly-once guarantee.
- `Block` should be reserved for small, trusted subscriber sets.
- Lossy policies must not be used for events whose loss would violate correctness.

## Comparison

| Variant                | Delivery model                 |   Goroutines | Backpressure                         | Error path                            | Best fit                        |
|------------------------|--------------------------------|-------------:|--------------------------------------|---------------------------------------|---------------------------------|
| Single-consumer stream | One channel, competing readers | 1 dispatcher | Bounded channel; blocks cancellably  | `Start` plus stream termination       | Simple and general integrations |
| Serialized handler     | One callback at a time         | 1 dispatcher | Handler duration                     | Handler error via `Err`               | Direct, bounded side effects    |
| Bounded pub/sub hub    | Copy to every subscriber       | 1 dispatcher | Per-subscriber block/drop/disconnect | Start/subscription errors and metrics | Highly concurrent listener sets |

## Recommendation

The **single-consumer event stream** is the recommended default. Channels compose naturally with `select`, contexts, and
existing Go service loops; its cancellation and ownership rules are easy to explain; and its small synchronization
surface makes race and leak freedom easier to verify. It is also the best option for simpler cases.

Do not use it when multiple independent consumers must each observe every event, or when caller code should be invoked
directly. Use the serialized handler for a single small, cancellation-aware action. Use the bounded pub/sub hub for
highly concurrent pub/sub cases, especially when slow consumers must be isolated with an explicit overload policy.

Regardless of variant, the package documentation must avoid claiming lossless OS-level delivery: Unix and Go runtime
signal delivery may coalesce repeated signals. The package can guarantee only deterministic mapping, lifecycle behavior,
and delivery policy for signals it actually observes.

## Execution metadata

- Model and requested effort identifier: `codex-5.5-medium`
- Planning/default phase timing: unavailable from the execution environment
- Input, output, cached, and reasoning token counts: unavailable from the execution environment

