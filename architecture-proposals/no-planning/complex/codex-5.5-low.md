# OS Signal Event Adapter Architecture Proposals

Model/effort: Codex 5.5, low.

## Scope and assumptions

The proposed package is named `signalevent`. It targets Unix with Go 1.26. The event type is generic and must be
comparable only where an implementation needs comparison; the designs below do not require that constraint. A mapping
may map several signals to the same event. OS signal delivery is inherently coalesced by the runtime, so none of these
APIs claims one event per signal occurrence.

All variants use an internal, unexported signal-source seam:

```go
type signalSource interface {
Notify(chan<- os.Signal, ...os.Signal)
Stop(chan<- os.Signal)
}
```

Production code adapts `os/signal.Notify` and `signal.Stop`; tests inject a fake. The context variant may instead adapt
`signal.NotifyContext`, as described below. Constructors copy mappings and signal slices, preventing later caller
mutation.

Common errors are intentionally small:

```go
var (
ErrAlreadyStarted = errors.New("signal listener already started")
ErrNotStarted = errors.New("signal listener not started")
ErrStopped        = errors.New("signal listener stopped")
ErrInvalidMapping = errors.New("invalid signal mapping")
ErrClosed = errors.New("subscription closed")
)
```

Validation errors wrap `ErrInvalidMapping` with the offending index or signal. Constructors reject nil or empty
mappings, nil signal keys, and duplicate signals in slice-based configuration. A map cannot contain duplicate keys.
Platform validity is checked by the production registration adapter where feasible; Unix signal availability is
build-tagged when constants differ. Unknown signals received from a fake or shared source are ignored and may increment
an optional internal metric. Multiple signals mapping to one event are valid.

## Comparison

| Variant                 | Delivery                | Slow-consumer policy             | Complexity | Best fit                             |
|-------------------------|-------------------------|----------------------------------|------------|--------------------------------------|
| 1. Single stream        | One consumer per value  | Bounded blocking                 | Low        | Simple application lifecycle         |
| 2. Handler dispatcher   | All matching handlers   | Worker isolation, bounded queue  | Medium     | Callback-oriented integration        |
| 3. Context event stream | One consumer per value  | Bounded blocking or cancellation | Low-medium | Context-native request/process scope |
| 4. Pub/sub broker       | Every active subscriber | Per-subscriber bounded policy    | High       | Highly concurrent fan-out            |

## Variant 1: Single-consumer event stream

### Description and intended use

One object owns one signal registration and one output stream. It suits a main goroutine or supervisor selecting over
lifecycle events and other channels. Although several goroutines can receive from the channel, each value goes to
exactly one receiver; fan-out is not promised.

### Public API

```go
type Stream[T any] struct { /* unexported */ }

func NewStream[T any](mapping map[os.Signal]T, buffer int) (*Stream[T], error)
func (s *Stream[T]) Start() (<-chan T, error)
func (s *Stream[T]) Stop() error
func (s *Stream[T]) Done() <-chan struct{}
```

An internal constructor accepts `signalSource`. `buffer` must be non-negative and is capped by a documented
implementation limit.

### Components and concurrency

A mutex protects state, the once-created signal channel, output channel, stop channel, and terminal error. `Start`
locks, changes `New` to `Running`, creates all channels, registers signals, then starts exactly one pump goroutine.
Publication occurs before unlocking. `Stop` locks and changes `Running` to `Stopping`, calls source cleanup exactly
once, closes the stop channel exactly once, unlocks, and waits for `Done`. Only the pump closes the output and `Done`
channels. This single-closer rule prevents send/close races.

Concurrent `Start` calls have one winner. Every later call returns `ErrAlreadyStarted`, including after stop. Concurrent
`Stop` calls are idempotent once running: all wait for the same `Done` and return the same result. `Stop` in `New`
returns `ErrNotStarted`.

### Lifecycle

`New -> Running -> Stopping -> Stopped`.

- `Start`: valid only in `New`.
- `Stop`: valid in `Running` or `Stopping`; in `Stopped` it returns nil.
- `Done`: always valid; its channel is allocated by the constructor and closes at `Stopped`.

There is no reset and no transition back to `New`.

### Registration, stop, and delivery

The pump receives signals from a buffered internal channel. Registration occurs before the pump starts; failure-capable
custom sources must register before state publication or roll construction back without consuming the single start. The
standard adapter does not return an error. On stop, `signal.Stop` is called before shutdown is requested, preventing new
runtime deliveries.

Mapped events are sent in receive order. Duplicate received signals produce duplicate sends, subject to OS coalescing.
Delivery is guaranteed for signals already accepted by the pump while running, unless cancellation wins. Default
shutdown semantics are **discard pending**: after observing stop, the pump exits without draining either internal
channel and closes output. This makes `Stop` bounded even if the consumer is absent.

Backpressure is bounded blocking: the pump blocks when output is full, but its send selects on the stop channel, so
`Stop` always releases it. The internal signal channel should have a documented small capacity (for example, one per
mapped signal); the OS/runtime may coalesce signals when it fills. No delivery error is returned because delivery is
asynchronous.

### Errors, tests, pros, cons, risks, limitations

Test with a fake source for registration arguments, mapping, ordering, buffer saturation, stop while blocked,
cleanup-once, channel closure, and all lifecycle transitions. Stress concurrent `Start`/`Stop` under `go test -race`. A
small subprocess integration test may send a safe Unix signal; use build tags and never use fatal defaults without
registration.

Pros:

- Small, idiomatic API and clear channel ownership.
- One goroutine and straightforward leak prevention.
- Natural use in `select`.

Cons:

- No broadcast semantics.
- A slow consumer delays all event processing.
- Shutdown discards buffered work.

Risks:

- Users may incorrectly assume the OS preserves signal multiplicity.
- An unbounded or non-cancellable send would leak; implementation review must preserve the stop arm.

Usage limitations:

- Not suitable when every listener must see every event.
- Not suitable for guaranteed audit delivery or durable processing.
- Receiving from multiple goroutines load-balances rather than broadcasts.

## Variant 2: Callback dispatcher

### Description and intended use

Users register handlers before start. Every received mapped event is offered to every registered handler in registration
order. This fits embedding in systems whose components already expose callback methods.

### Public API

```go
type Handler[T any] func (context.Context, T) error

type Dispatcher[T any] struct { /* unexported */ }

func NewDispatcher[T any](mapping map[os.Signal]T, queueSize int) (*Dispatcher[T], error)
func (d *Dispatcher[T]) Handle(h Handler[T]) error
func (d *Dispatcher[T]) Start(ctx context.Context) error
func (d *Dispatcher[T]) Stop() error
func (d *Dispatcher[T]) Done() <-chan struct{}
func (d *Dispatcher[T]) Wait() error
```

`Handle(nil)` is invalid. At least one handler is required at `Start`.

### Components and concurrency

A mutex protects lifecycle and the pre-start handler slice. Handlers become immutable at start and are copied into
pump-owned state. One signal pump maps signals into a bounded work queue. A fixed worker set invokes handlers; its size
is an internal conservative default or a validated constructor option. A `WaitGroup` tracks pump and workers. A
finalizer goroutine waits for them, closes `Done`, and records joined handler errors.

`Handle` is valid only in `New`; otherwise it returns `ErrAlreadyStarted`. This intentionally avoids complicated
unregister/in-flight semantics. `Start` uses the same lifetime single-start rule. Context cancellation and `Stop`
converge on one `sync.Once` stop path that unregisters signals and cancels the internal context. Handlers receive that
context and must honor it.

### Lifecycle

`New -> Running -> Stopping -> Stopped`, with the same `Start` and `Stop` rules as Variant 1. `Wait` before start
returns `ErrNotStarted`; during or after start it waits and returns the accumulated error.

### Delivery and backpressure

The dispatcher snapshots the handler list for each queued event. Every handler is invoked once per dequeued event.
Events retain FIFO dequeue order, but multiple workers mean handler completion order is not guaranteed. For strict
ordering, configure one worker.

The work queue is bounded. Default policy blocks the pump while selecting on cancellation. This isolates signal
reception only up to the internal signal buffer; subsequent OS signals may coalesce. A documented option may select
`DropNewest`, but it must expose a drop callback or counter. Returning per-event delivery errors is impossible to the
signal sender. Handler errors do not stop dispatch by default; `Wait` returns `errors.Join` of a bounded number of
representative errors, while a hook receives all errors. Panic recovery is optional and, if offered, converts a panic to
an error; otherwise document that a handler panic terminates the process.

At stop, queued-but-not-started work is discarded. In-flight handlers are cancelled and awaited. Because Go cannot
forcibly terminate handlers, `Stop` can block forever if a handler ignores context. Provide `Wait`; do not pretend a
timeout proves cleanup.

### Tests, pros, cons, risks, limitations

Tests cover handler snapshotting, invocation counts, ordering with one worker, concurrency with several workers, errors,
cancellation, queue saturation, and a deliberately blocked but context-aware handler. Run race tests and subprocess
signal integration as above.

Pros:

- Direct fan-out without exposing channels.
- Handler errors can be observed centrally.
- Fixed workers bound concurrency.

Cons:

- Handler cooperation is required for prompt shutdown.
- Completion ordering varies with multiple workers.
- More policy choices than a stream.

Risks:

- A context-ignoring handler causes shutdown/goroutine leaks.
- Error accumulation must be bounded.
- Callback reentrancy can deadlock if handlers call blocking lifecycle methods; document that `Stop` must not be called
  synchronously from a handler.

Usage limitations:

- Registration is pre-start only; no dynamic subscription.
- Not appropriate for untrusted handlers.
- Not a durable job executor and cannot guarantee one event per physical signal.

## Variant 3: Context-oriented event stream

### Description and intended use

This variant binds the listener lifetime to a parent context and uses a returned context-like session for completion. It
is best when cancellation already flows through a process or component tree.

### Public API

```go
type Session[T any] struct { /* unexported */ }

func NewSession[T any](mapping map[os.Signal]T, buffer int) (*Session[T], error)
func (s *Session[T]) Start(parent context.Context) (<-chan T, error)
func (s *Session[T]) Stop() error
func (s *Session[T]) Done() <-chan struct{}
func (s *Session[T]) Err() error
```

A nil parent is rejected; callers use `context.Background()` explicitly.

### Components and concurrency

State and channel ownership match Variant 1. Production may create one `signal.NotifyContext(parent, signals...)`,
retain its stop function, and run a pump selecting on the notification context and a raw signal channel only if event
identity is needed. Because `NotifyContext` alone reports only cancellation and not which of several signals fired,
distinct event remapping requires either one notification context per equivalence class (signals mapping to the same
event) or, preferably, the common `signalSource` channel plus `context.WithCancelCause`. The latter is simpler and
avoids one goroutine/context per mapping.

The parent watcher is not a separate goroutine: the pump selects on `parent.Done()`. `Stop` invokes the same `sync.Once`
cancellation path. Source cleanup precedes internal cancellation. Only the pump closes events and `Done`.

### Lifecycle, delivery, and errors

The state machine is identical to Variant 1. Parent cancellation moves `Running -> Stopping -> Stopped`; subsequent
`Stop` returns nil. `Err` returns nil before termination, then `context.Cause(parent)` for parent cancellation, or an
internal stop cause for explicit stop. Explicit normal stop should be distinguishable by a documented sentinel only if
callers need it; otherwise it returns nil.

Delivery is single-consumer FIFO, bounded blocking, cancellation-aware, and discard-on-stop. Channel ownership and
OS-coalescing caveats match Variant 1. A cancellation-ready select may choose either a pending send or cancellation when
both are ready, so the exact shutdown boundary is intentionally unspecified.

### Tests, pros, cons, risks, limitations

Use fake contexts and a fake source; verify already-cancelled parents, cancellation causes, concurrent explicit
stop/parent cancel, blocked output, cleanup once, and race safety. Test the optional `NotifyContext` adapter separately
and ensure its returned stop function is called.

Pros:

- Composes naturally with structured cancellation.
- Minimal goroutine count and no hidden watcher.
- Clear cause propagation.

Cons:

- Context does not itself carry typed events.
- The API is close to Variant 1 and adds lifecycle-cause complexity.
- Pure `NotifyContext` is awkward for distinct remappings.

Risks:

- Failing to call the `NotifyContext` stop function leaks signal registration.
- Users may treat context values as an event bus; explicitly prohibit that design.

Usage limitations:

- Single-consumer only.
- No delivery after parent cancellation and no drain guarantee.
- Use Variant 1 when context causes add no value.

## Variant 4: Dynamic pub/sub broker

### Description and intended use

A broker broadcasts each mapped event to every active subscription. Subscribers can join and leave while running, each
with an independent bounded buffer and overflow policy. This is the highly concurrent option.

### Public API

```go
type OverflowPolicy uint8
const (
Block OverflowPolicy = iota
DropNewest
DropOldest
)

type Subscription[T any] struct { /* unexported */ }
func (s *Subscription[T]) Events() <-chan T
func (s *Subscription[T]) Close() error
func (s *Subscription[T]) Dropped() uint64

type Broker[T any] struct { /* unexported */ }
func NewBroker[T any](mapping map[os.Signal]T) (*Broker[T], error)
func (b *Broker[T]) Subscribe(buffer int, policy OverflowPolicy) (*Subscription[T], error)
func (b *Broker[T]) Start(ctx context.Context) error
func (b *Broker[T]) Stop() error
func (b *Broker[T]) Done() <-chan struct{}
```

### Components and concurrency

One broker goroutine exclusively owns the subscription map and is the only goroutine that sends to or closes
subscription event channels. Subscribe/unsubscribe requests travel over command channels and carry response channels.
This actor ownership eliminates send/close races. A mutex protects only lifecycle and safely publishes command channels;
it is never held while waiting for the actor.

Before start, `Subscribe` records immutable specifications under the lifecycle mutex; at start they are transferred to
the actor. While running, commands are cancellation-aware and acknowledged only after application. During
stopping/stopped, new subscriptions return `ErrStopped`. `Subscription.Close` is idempotent; it returns after the actor
removes and closes the channel. Concurrent close and broker stop converge at the actor. No caller closes `Events`.

### Lifecycle and cleanup

`New -> Running -> Stopping -> Stopped`. `Start` remains lifetime single-use. `Stop` unregisters the source, signals the
actor, and waits. The actor stops accepting commands, closes every subscription channel, discards queued events, and
closes broker `Done`. Requests racing with stop either receive an acknowledgement or `ErrStopped`; every request select
includes `Done`, preventing stranded callers.

### Delivery, ordering, and backpressure

Each event is offered to all subscriptions active when the actor processes it. Per subscriber, order matches actor
processing order. Cross-subscriber observation times are not synchronized. Duplicate received signals yield duplicate
offers, subject to OS coalescing.

- `Block`: guarantees offer to active subscribers while running but one slow subscriber stalls all. Every send selects
  on stop.
- `DropNewest`: non-blocking; preserves already-buffered events and increments an atomic drop counter.
- `DropOldest`: actor non-blockingly removes one buffered item, then sends the new item. Because the actor is the sole
  sender but receivers consume concurrently, the implementation must tolerate the receiver winning the race; retry once
  without blocking or count the new event dropped.

Policies are per subscription. For isolation without global stalls, recommend bounded `DropOldest` or `DropNewest`. A
goroutine per delivery is prohibited: it breaks ordering, bounds, and leak guarantees.

### Errors, validation, and tests

Reject negative buffers, unknown policies, and `Block` with a zero buffer unless the caller explicitly accepts
rendezvous semantics. `Subscribe` returns lifecycle errors; asynchronous drops are reported through `Dropped`, not as
errors.

Deterministic actor tests cover subscribe/close races, membership boundaries, each overflow policy, per-subscriber
ordering, slow-subscriber isolation, broker stop with pending commands, and closure ownership. Stress with hundreds of
subscribers under `go test -race`. Integration and mapping tests follow the common strategy.

Pros:

- Correct dynamic broadcast semantics.
- Explicit per-subscriber backpressure and drop accounting.
- Actor ownership makes channel closing auditable.

Cons:

- Largest implementation and testing surface.
- Blocking policy can still create global head-of-line blocking.
- Membership boundaries are defined by actor processing, not wall-clock call time.

Risks:

- Command paths missing a `Done` select can leak callers.
- Drop-oldest races require careful accounting.
- Large subscriber counts multiply memory use by configured buffer sizes.

Usage limitations:

- No replay for late subscribers.
- No durable delivery or global acknowledgement.
- Do not use when a single receiver suffices.

## Recommendation

Use **Variant 1, the single-consumer event stream**, as the default. It has the smallest exported surface, one clear
channel owner, one pump, deterministic FIFO behavior, and a cancellation path that cannot remain blocked on output. It
is sufficient for the common case where a process supervisor translates signals into application lifecycle events.

Do not use it when all independent consumers must observe every event; choose Variant 4 for dynamic, highly concurrent
pub/sub. Variant 2 is preferable when existing components naturally expose context-aware handlers and central error
reporting matters. Variant 3 is the best context-native choice, but for the simplest cases Variant 1 remains preferable
because `Stop` plus `Done` already expresses the lifecycle without cancellation-cause semantics.

## Implementation checklist

- Copy and validate mappings; allow many signals per event and document OS coalescing.
- Protect lifecycle transitions and enforce lifetime single-start.
- Register before pumping; unregister exactly once before cancellation.
- Give exactly one internal owner permission to close every public channel.
- Make every potentially blocking send and command cancellation-aware.
- Define shutdown as discard-pending unless a future API explicitly offers draining.
- Bound buffers, worker counts, error retention, and subscriber memory.
- Inject the signal source; run lifecycle, saturation, cancellation, integration, and race tests.
- Use Unix build tags for platform-specific constants and tests.
- Document all exported identifiers; keep interfaces internal and small; avoid globals, reflection, `unsafe`, and
  stuttering names.

## Execution metadata

The work consisted of prompt review, architecture design, and document authoring in one default-mode phase. Exact phase
timing and token accounting are not exposed by the execution environment, so no unverifiable figures are reported.
