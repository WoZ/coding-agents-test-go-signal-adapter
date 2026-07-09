# Architecture proposals: Unix signal-to-event adapter

## Scope and assumptions

This document designs a Go 1.26 package, tentatively named `sigevent`, for Unix systems. It maps process signals to
caller-defined values without exposing `os/signal` mechanics in its public API.

The analysis is based only on the task specification. No repository implementation or previous proposal is assumed.

The following assumptions apply to all variants:

- One adapter object represents one signal-listening lifetime. A call that passes public precondition checks and begins
  startup consumes that lifetime, whether source registration succeeds or fails. A precondition error, such as starting
  a callback dispatcher with no handlers, may be corrected while the object remains `New`. Once startup begins, the
  object can never return to `New`.
- `Stop` is safe to call concurrently. Before any start attempt it returns `ErrNotStarted`; after a start attempt it is
  idempotent.
- Process signals are notifications, not a durable message queue. The operating system and `os/signal` may coalesce
  repeated signals. A package can promise one mapped event per signal value received from its signal source, but cannot
  promise one event per signal sent by the OS.
- Mapping values have type parameter `T any`. The package shallow-copies the map, but cannot deep-copy arbitrary `T`. A
  caller that maps signals to pointers, slices, maps, or otherwise mutable values must not mutate those values
  concurrently with delivery.
- Multiple signals may map to equal values or the same pointer. No equality constraint is placed on `T`.
- Delivery ends when stopping begins. A signal already received may either finish delivery or be discarded at the
  documented cancellation point for the selected variant. No variant drains indefinitely during shutdown.
- The production source is process-global in effect because `os/signal` is process-wide, even though package state is
  not global. Multiple adapter objects may register the same signal and each may receive it. Applications must
  coordinate adapters if that is undesirable.
- `SIGKILL` and `SIGSTOP` cannot be caught and are rejected. Other accepted signals are determined by per-`GOOS`
  validation files.
- A callback that ignores its canceled context and never returns is defective caller code. Go cannot forcibly terminate
  it. The callback variant is leak-free only when handlers obey their context contract.

## Common design rules

### Mapping and signal validation

Constructors validate configuration before creating an object:

1. A nil or empty mapping returns a configuration error wrapping `ErrNoMapping`.
2. A nil `os.Signal` key returns a configuration error identifying the offending entry.
3. Every key must be accepted by an unexported, build-tagged `validateSignal` implementation for the current Unix
   target. Uncatchable and unsupported signals return an error wrapping `ErrUnsupportedSignal`.
4. Map-based APIs cannot contain duplicate keys. If a future convenience API accepts `[]Binding[T]`, it must reject
   duplicate signals rather than silently use the last value.
5. Multiple signals mapping to one event are valid.
6. Signals absent from the copied mapping are ignored defensively. In normal operation they are not registered and
   therefore do not arrive.
7. The constructor copies the map and derives a stable signal slice. Later mutation of the caller's map cannot alter
   registration or mapping.

The package should support only Unix targets that have an explicit validator. A strict build-tagged allowlist is
preferable to claiming that every numeric `syscall.Signal` is valid. The allowlist can be expanded deliberately for
platform-specific or real-time signals. This makes rejection predictable at the cost of not accepting every kernel
extension immediately.

### Internal signal source

All variants depend on a small unexported interface:

```go
type signalSource interface {
Notify(chan<- os.Signal, ...os.Signal) error
Stop(chan<- os.Signal)
}
```

The production implementation delegates to `signal.Notify` and `signal.Stop`. Its `Notify` returns `nil` after
constructor validation; the error exists so deterministic fakes can simulate startup failure. An unexported constructor
accepts a source in package tests. Keeping this interface private avoids making `os.Signal` or registration mechanics
part of the supported API.

The goroutine that receives source signals owns cleanup. On exit it calls `source.Stop(signalCh)` before closing any
user-visible output. It never calls `signal.Reset`, because reset would affect unrelated process registrations. The
source channel is buffered, is never closed by this package, and becomes unreachable after `Stop` and the worker exit.

### Shared errors

The exported error surface should remain small:

```go
var (
ErrStarted = errors.New("signal adapter has already been started")
ErrNotStarted = errors.New("signal adapter has not been started")
ErrStopping          = errors.New("signal adapter is stopping")
ErrStopped = errors.New("signal adapter has stopped")
ErrNoMapping = errors.New("signal mapping is empty")
ErrUnsupportedSignal = errors.New("signal is unsupported")
ErrNilContext = errors.New("context is nil")
)

type ConfigError struct {
Field string
Err   error
}

func (e *ConfigError) Error() string
func (e *ConfigError) Unwrap() error
```

Callers inspect lifecycle errors with `errors.Is`. `ConfigError` adds the field name without creating an elaborate
hierarchy. A failed internal registration is returned as `fmt.Errorf("register signals: %w", err)`.

The concrete types require their constructors because a valid mapping and initialized lifecycle channels are mandatory;
zero-value usability is not practical here. Methods with error results should defensively return an
initialization/configuration error rather than panic when called on a zero value. This is a usage guard, not an
alternative initialization path.

### Baseline lifecycle

The implementation uses these states internally:

```text
New -> Starting -> Running -> Stopping -> Stopped
          \------------------------------^
```

`Starting` is short-lived because production registration is synchronous, but it is useful for defining concurrent calls
and fake-source failures. A single mutex protects the state, the stop-request flag, the terminal error, registration
collections, and references to lifecycle channels. Channels and immutable mapping data are safely published before state
becomes `Running`.

The common transitions are:

| Method/current state | New                   | Starting                                     | Running                   | Stopping     | Stopped      |
|----------------------|-----------------------|----------------------------------------------|---------------------------|--------------|--------------|
| `Start`              | attempts registration | `ErrStarted`                                 | `ErrStarted`              | `ErrStarted` | `ErrStarted` |
| `Stop(ctx)`          | `ErrNotStarted`       | waits for startup, requests stop, then waits | requests stop, then waits | waits        | `nil`        |
| `Done()`             | open                  | open                                         | open                      | open         | closed       |

The implementation may hold the lifecycle mutex while invoking the package-owned source's synchronous `Notify`; this
prevents `Stop` from racing registration. It must not hold that mutex while waiting for a goroutine or user code. If
registration fails, the object transitions directly to `Stopped`, records the error, closes `done`, and all later
`Start` calls return `ErrStarted`.

`Stop(ctx)` rejects a nil context. The first stopping caller closes an internal `stop` channel under the mutex and
transitions to `Stopping`; all callers then wait on either `done` or their own context. A caller timeout returns
`ctx.Err()` but does not undo the stop request. Cleanup continues, and a later `Stop` may wait again. Once started,
`Stop` returning `nil` means signal delivery has been unregistered and all package-owned goroutines for that object have
exited.

The exact additional operations and terminal-error behavior differ by variant.

## Variant 1: single-consumer channel listener

### Description and intended use

`Listener[T]` is the smallest general-purpose design. `Start` returns one receive-only event channel. It fits
command-line programs and services with one event loop that selects over signals and other inputs.

### Public API sketch

```go
package sigevent

type ListenerConfig[T any] struct {
	Mapping      map[os.Signal]T
	EventBuffer  int
	SignalBuffer int
}

type Listener[T any] struct {
	// unexported
}

func NewListener[T any](cfg ListenerConfig[T]) (*Listener[T], error)

// Start registers signals and starts delivery. It succeeds at most once.
func (l *Listener[T]) Start() (<-chan T, error)

// Stop requests shutdown and waits for cleanup or ctx cancellation.
func (l *Listener[T]) Stop(ctx context.Context) error

// Done is closed after signal registration and the worker have been cleaned up.
func (l *Listener[T]) Done() <-chan struct{}
```

`EventBuffer` and `SignalBuffer` must be non-negative. Reasonable defaults can be represented by zero only if zero is
documented as unbuffered; hidden default substitution makes backpressure harder to reason about. A convenience
constructor could supply explicit defaults, but should not use functional options for three fields.

### Core components and concurrency model

- An immutable copied `map[os.Signal]T`.
- One mutex-protected lifecycle record.
- A source channel sized by `SignalBuffer`.
- One package worker goroutine.
- One package-owned event channel sized by `EventBuffer`.
- `stop` and `done` broadcast channels, each closed exactly once by designated owners.

After successful registration, the worker receives signals, looks up the immutable map, and sends the mapped event:

```go
select {
case events <- event:
// Delivered to the channel buffer or a waiting receiver.
case <-stop:
// The current event is discarded and cleanup starts.
}
```

The worker is the only sender and the only closer of `events`. Public methods never close it. The caller receives but
must never close it. `done` is closed after `source.Stop` and after `events` is closed. The stop path is always
selectable even when event delivery is blocked, preventing a slow or absent consumer from leaking the worker.

All exported methods are concurrency-safe. There is no `Subscribe` operation. Multiple goroutines technically can
receive from the returned channel, but then each event goes to exactly one competing receiver and assignment is
scheduler-dependent.

### Lifecycle and cancellation

This variant uses the baseline state machine. `Start` allocates and publishes all channels, registers the source, starts
the worker, sets `Running`, and returns the same receive-only channel on its successful call only. Later calls return a
nil channel and `ErrStarted`.

On stop, the worker selects the stop request, calls `source.Stop`, discards signals still buffered in the source
channel, closes `events`, records `Stopped`, and closes `done`. If a mapped event has already entered `events`, it
remains readable after close; Go channel close naturally permits draining buffered values. An event waiting in the
worker's cancellation-aware send may be sent or discarded if both send and stop are ready; callers must treat stop as
the delivery boundary, not as a transactional cutoff.

### Delivery, backpressure, and ordering

- Each source notification produces at most one event.
- One event is consumed by one receiver. This is work distribution, not broadcast.
- Source receive order is preserved by the single worker.
- Duplicate source notifications produce duplicate events, subject to OS and `os/signal` coalescing before the worker
  receives them.
- Delivery is blocking while running. A full event buffer applies backpressure to the worker.
- While blocked, the source buffer may fill and the runtime may coalesce or lose later signal notifications. This is
  inherent to bounded signal handling.
- No package-level drop policy is applied before stop. If loss behavior is preferable, use Variant 4.
- Buffered events are retained and can be drained after the event channel closes. Signals not yet mapped when stop wins
  are discarded.

### Error and validation model

Constructor, lifecycle, and context errors use the common model. There is no asynchronous error channel because
production registration is the only expected fallible internal operation and it happens in `Start`. If the worker
encounters an impossible internal invariant, it should terminate and retain a terminal error for diagnostics in an
optional `Wait` method only if a real fallible operation is later introduced; adding `Wait` preemptively is unnecessary.

### Testing strategy

- Inject a fake source and assert exact mapping and event order.
- Verify nil, empty, externally mutated, unsupported, and many-to-one mappings.
- Test unbuffered and bounded delivery with a deliberately stalled receiver.
- Start many goroutines calling `Start`; exactly one succeeds.
- Race `Start` with `Stop`, and run many concurrent `Stop` calls with both live and canceled contexts.
- Assert `Stop` unblocks a worker stuck on event send, calls source `Stop` exactly once, closes `events` once, and
  closes `done` last.
- Assert buffered events remain drainable after channel close and source-buffered signals are discarded after the stop
  boundary.
- Run all concurrency tests under `go test -race`.
- Use a small subprocess integration test that sends a catchable signal to itself. Do not send signals to the test
  runner process. Select signal constants with build tags and skip on unsupported Unix targets.

### Pros

- Small, idiomatic API with clear channel ownership.
- Minimal allocation and exactly one worker goroutine.
- Strong per-source ordering and straightforward deterministic tests.
- Cancellation remains responsive even with a stalled receiver.

### Cons

- Not broadcast; multiple receivers compete.
- Blocking backpressure can indirectly increase signal coalescing.
- The receive channel cannot return per-event errors.
- Shutdown has a deliberately non-transactional boundary when send and stop are simultaneously ready.

### Risks

- Users may incorrectly treat OS signals as durable events.
- Users may close or attempt to send on a channel they do not own if ownership documentation is ignored.
- Mutable mapped values can still be raced by caller code.

### Usage limitations

- Best for one logical consumer.
- Unsuitable when every subscriber must see every event.
- Unsuitable when slow consumers must be isolated or measured independently.
- Does not guarantee a one-to-one relationship between signals sent by the OS and events received.

## Variant 2: serialized callback dispatcher

### Description and intended use

`Dispatcher[T]` invokes an immutable set of handlers for each mapped event. It is useful when a service already
organizes lifecycle actions as functions and wants signal handling to stay behind a command-style API.

Handlers execute serially in registration order. This is an intentional semantic choice: it gives deterministic ordering
and avoids unbounded goroutine creation. It also makes handler latency part of signal-processing latency.

### Public API sketch

```go
type Handler[T any] func (context.Context, T) error

type DispatcherConfig[T any] struct {
Mapping      map[os.Signal]T
SignalBuffer int
}

type Dispatcher[T any] struct {
// unexported
}

func NewDispatcher[T any](cfg DispatcherConfig[T]) (*Dispatcher[T], error)

// Handle adds a handler while the dispatcher is New.
func (d *Dispatcher[T]) Handle(handler Handler[T]) error

// Start registers signals and starts dispatch. It succeeds at most once.
func (d *Dispatcher[T]) Start() error

// Stop cancels the active handler context and waits for cleanup or ctx cancellation.
func (d *Dispatcher[T]) Stop(ctx context.Context) error

// Wait waits for normal stop or a terminal handler failure.
func (d *Dispatcher[T]) Wait(ctx context.Context) error

func (d *Dispatcher[T]) Done() <-chan struct{}
```

Additional errors:

```go
var (
ErrNoHandlers = errors.New("dispatcher has no handlers")
ErrRunning = errors.New("signal adapter is running")
)

type HandlerError struct {
Index int
Err   error
}

func (e *HandlerError) Error() string
func (e *HandlerError) Unwrap() error
```

### Core components and concurrency model

- The common immutable mapping and lifecycle record.
- A handler slice protected by the lifecycle mutex while `New`, then immutable.
- One root handler context canceled on stop or terminal failure.
- One worker that receives and dispatches.
- A source channel plus `stop` and `done`.

`Handle` rejects a nil handler with `ConfigError`. It is valid only in `New`. Concurrent `Handle` calls are serialized
and preserve lock-acquisition order. A concurrent `Handle` and `Start` has an explicit result: if `Handle` gets the lock
first it is included; otherwise it returns `ErrRunning`. In `Stopping` and `Stopped`, it returns `ErrStopping` and
`ErrStopped`, respectively. `Start` returns a configuration error wrapping `ErrNoHandlers` if no handler has been
installed, and that validation occurs before the start attempt consumes the object.

There is no runtime unsubscribe. Immutability avoids invoking a handler after a caller believed it had been removed and
keeps the callback contract implementable without per-handler workers.

For each received signal, the worker invokes handlers in registration order with the root context. It does not hold an
internal mutex while calling user code. The next signal is not dispatched until every handler for the current event has
returned.

### Lifecycle, cancellation, and terminal failure

The baseline state machine applies. `Handle` is valid only in `New`.

`Stop` closes the stop request and cancels the handler context before waiting. A handler is required to stop promptly
when its context is canceled. Once it returns, the worker unregisters the source and closes `done`.

If a handler returns an error, dispatch stops immediately. Remaining handlers do not receive that event. The worker
stores `*HandlerError`, cancels the root context, unregisters signals, and transitions through `Stopping` to `Stopped`.
`Wait` returns that terminal error. An explicit, successful stop causes `Wait` to return `nil`.

The worker wraps each handler call with a narrow `recover`. A panic is converted to a `HandlerError` whose cause
includes the recovered value and stack, then follows the same terminal path. Recovering at this boundary prevents
package cleanup from being skipped; it is not an instruction to continue after unknown handler state.

If the context passed to `Wait` or `Stop` expires first, that call returns `ctx.Err()`. The adapter continues
cancellation and cleanup in the background.

### Delivery, backpressure, and ordering

- Every registered handler is intended to see every mapped event.
- Handlers are called serially for an event and events are processed serially.
- Ordering is source order, then handler registration order.
- Delivery is blocking and at-most-once per handler invocation. The package does not retry failed handlers.
- A slow handler backpressures all later handlers and events. The bounded source channel is the only burst absorption.
- On handler failure, undispatched handlers and queued signals are discarded.
- On explicit stop, the active handler gets cancellation; no new handler invocation begins after the worker observes
  stop.

This variant deliberately does not offer drop-newest, drop-oldest, or a worker pool. Those choices would weaken ordering
and make callback concurrency part of the base contract. Variant 4 is the better fit for isolated subscribers.

### Error and validation model

Configuration and lifecycle use common errors. Handler errors are terminal and retain their cause for `errors.Is`/
`errors.As`. `Wait(nil)` and `Stop(nil)` return `ErrNilContext`.

`Wait` in `New` returns `ErrNotStarted`. In `Starting`, `Running`, or `Stopping`, it waits. In `Stopped`, it immediately
returns the retained terminal error, if any.

### Testing strategy

- Verify handler registration order and event order with a fake source.
- Race many `Handle` calls with `Start`; assert only pre-start successful handlers execute.
- Verify `Start` with no handlers does not consume the object, then add a handler and start successfully.
- Use a blocking, context-aware handler to verify `Stop` cancels it and cleanup finishes.
- Verify returned handler errors and panics become terminal `HandlerError` values and that the source is always
  unregistered.
- Verify no handler after the failing handler runs for that event.
- Verify repeated and concurrent `Wait` and `Stop` calls.
- Exercise backpressure with a slow handler and a full fake source buffer.
- Run under the race detector and use subprocess-only real-signal integration tests.

### Pros

- Natural integration with command-oriented application code.
- Deterministic handler and event ordering.
- No exposed event channels and no unbounded goroutine creation.
- Handler failure has a clear, observable terminal meaning.

### Cons

- One slow handler delays everything.
- Runtime add/remove is intentionally unsupported.
- Correct shutdown depends on handlers respecting context cancellation.
- A handler failure prevents remaining handlers from seeing the event.

### Risks

- A handler that blocks forever while ignoring context prevents full cleanup; Go provides no safe forced termination.
- Callback code may re-enter the dispatcher. Calling `Stop` synchronously from a handler would deadlock while waiting
  for that same handler to return.
- Panic recovery protects cleanup but cannot undo side effects from a partially executed handler.

### Usage limitations

- A handler must not call blocking `Stop` or `Wait` on its own dispatcher. It may trigger cancellation from another
  goroutine or return an error.
- Handlers must honor context cancellation and should finish quickly.
- Not suitable for dynamic subscriptions, independent backpressure, or concurrent handler execution.

## Variant 3: context-scoped pull receiver

### Description and intended use

`Receiver[T]` hides channels behind a blocking `Receive(ctx)` operation. A root context controls the listening lifetime,
while each receive can have its own deadline. This fits structured-concurrency code that already expresses all blocking
operations with contexts and wants explicit receive errors instead of channel-close conventions.

This design uses `signal.Notify`, not `signal.NotifyContext`, for registration. `NotifyContext` reports only
cancellation and does not identify which signal caused it. Creating one notify-context per mapped signal would require
one registration and goroutine per signal and would not give a meaningful cross-signal order. It is therefore a poor
primitive for arbitrary signal-to-value mapping. Context still defines lifetime and pull cancellation at the public
boundary.

### Public API sketch

```go
type ReceiverConfig[T any] struct {
Mapping      map[os.Signal]T
QueueSize    int
SignalBuffer int
}

type Receiver[T any] struct {
// unexported
}

func NewReceiver[T any](cfg ReceiverConfig[T]) (*Receiver[T], error)

// Start binds the receiver lifetime to ctx. The first call consumes the object.
func (r *Receiver[T]) Start(ctx context.Context) error

// Receive returns the next event or a context/lifecycle error.
func (r *Receiver[T]) Receive(ctx context.Context) (T, error)

func (r *Receiver[T]) Stop(ctx context.Context) error
func (r *Receiver[T]) Done() <-chan struct{}
```

Additional error:

```go
var ErrConcurrentReceive = errors.New("another Receive call is active")
```

### Core components and concurrency model

- The common mapping, source, lifecycle mutex, and worker.
- A root context watcher integrated into the worker select.
- A bounded internal `events` queue owned by the worker.
- An atomic compare-and-swap guard allowing only one active `Receive`.

`Start` rejects a nil context. If the context is already canceled, it returns `ctx.Err()`, transitions to `Stopped`, and
consumes the object's lifetime. Otherwise, it registers signals, starts the worker, and returns after registration
succeeds.

The worker maps source notifications and performs cancellation-aware sends to the internal queue. `Receive` uses an
atomic guard so a second overlapping call fails immediately with `ErrConcurrentReceive`; it never silently load-balances
events between callers. It then selects among the queue, its call context, and `done`. It carefully rechecks the queue
after `done` so buffered events are returned before `ErrStopped`.

The queue is never exposed. The worker is its sole sender and closer. `Receive` only receives. The atomic guard controls
API usage, not lifecycle state; the mutex remains the authority for state and terminal error.

### Lifecycle and cancellation

The baseline lifecycle applies with two cancellation sources:

- Root context cancellation is equivalent to an asynchronous stop request.
- Explicit `Stop(waitCtx)` requests the same stop and lets a caller independently bound the wait.

Whichever occurs first closes the internal stop path exactly once. Canceling an individual `Receive` does not stop the
receiver and does not consume an event unless the queue receive won the select. As with all Go selects, if both an event
and cancellation are ready, either may win; callers requiring a hard deadline cutoff must inspect their context after
receipt and define application policy.

During shutdown, the worker unregisters signals and closes the internal queue. Already queued events remain available
through `Receive`; after they are drained, `Receive` returns `ErrStopped`. A `Receive` blocked with a live context wakes
when the queue closes. A caller can distinguish its own timeout (`context.DeadlineExceeded`) from stream end (
`ErrStopped`).

### Delivery, backpressure, and ordering

- Exactly one successful `Receive` obtains each queued event.
- Only one `Receive` may be active, making source order explicit.
- The bounded queue blocks the worker when full, with a stop-select escape.
- Duplicate source inputs remain duplicates unless coalesced upstream.
- Root cancellation stops accepting new inputs. Buffered mapped events remain pullable afterward.
- There is no drop policy. Queue pressure propagates to source receipt.

An alternative `Receive` policy could discard the queue immediately on stop, but retaining it is more useful and mirrors
closed buffered-channel behavior. The contract must not vary by stop source.

### Error and validation model

Constructor and lifecycle errors follow the common model. `Receive(nil)` and `Start(nil)` return `ErrNilContext`.
`Receive` before start returns `ErrNotStarted`. During stopping it may return a queued event, wait for final cleanup, or
return its own context error. After cleanup and queue drain it returns `ErrStopped`. It does not return `ErrStopping`,
because doing so would create a race-dependent drain contract.

The zero value for `T` accompanies every error. Documentation must tell callers to ignore the value when `err != nil`.

### Testing strategy

- Test mapping and order using a fake source and sequential receives.
- Test canceled root context before start, during idle receive, during a full queue, and concurrently with explicit
  stop.
- Test per-call receive deadline without stopping the adapter.
- Assert exactly one of two overlapping receives gets access and the other gets `ErrConcurrentReceive`.
- Verify buffered events drain after stop, followed by `ErrStopped`.
- Race `Start`, root cancellation, `Stop`, and `Receive` under `go test -race`.
- Verify source unregister occurs once and all internal goroutines terminate.
- Use build-tagged subprocess integration tests for real signals.

### Pros

- Context is present on every blocking public operation.
- No public channel ownership rules for callers to learn.
- Receive errors distinguish local cancellation from stream termination.
- Explicitly prevents accidental competing consumers.

### Cons

- More method calls and state checks than a channel `select`.
- Still uses an internal channel, so it is not mechanically simpler than Variant 1.
- One active receiver only.
- `signal.NotifyContext` cannot perform the actual remapping.

### Risks

- Users may assume context cancellation has a transactional priority over a simultaneously ready event; Go `select` does
  not provide that.
- Incorrect queue/drain implementation can lose the final buffered event or return `ErrStopped` too early.
- A large queue can retain mutable event values longer than expected.

### Usage limitations

- Intended for one pull consumer.
- Poor fit for callers that need to select directly over many channels.
- Not a broadcast system.
- Does not provide durable delivery or prevent upstream signal coalescing.

## Variant 4: bounded channel pub/sub hub

### Description and intended use

`Hub[T]` broadcasts every mapped event to a dynamic set of subscriptions. Each subscription has its own bounded channel
and explicit overflow policy, so a slow subscriber does not block source processing or other subscribers.

This is the best fit for highly concurrent services where independent components need the same lifecycle event and
subscriber churn is expected.

### Public API sketch

```go
type OverflowPolicy uint8

const (
DropNewest OverflowPolicy = iota
DropOldest
CancelSubscriber
)

type HubConfig[T any] struct {
Mapping      map[os.Signal]T
SignalBuffer int
}

type SubscriptionConfig struct {
Buffer   int
Overflow OverflowPolicy
}

type Hub[T any] struct {
// unexported
}

type Subscription[T any] struct {
// unexported
}

func NewHub[T any](cfg HubConfig[T]) (*Hub[T], error)
func (h *Hub[T]) Start() error
func (h *Hub[T]) Subscribe(cfg SubscriptionConfig) (*Subscription[T], error)
func (h *Hub[T]) Stop(ctx context.Context) error
func (h *Hub[T]) Done() <-chan struct{}

func (s *Subscription[T]) Events() <-chan T
func (s *Subscription[T]) Unsubscribe()
func (s *Subscription[T]) Dropped() uint64
```

`Subscription` values are created only by a hub. `Unsubscribe` is idempotent and concurrency-safe.

### Core components and concurrency model

- The common source worker and immutable mapping.
- One mutex protecting lifecycle state and the subscription map.
- A monotonically increasing internal subscription ID.
- One bounded channel per subscriber.
- An `atomic.Uint64` dropped counter in each subscription.
- No per-subscriber goroutine.

`Subscribe` is valid in `New` and `Running`; it returns `ErrStopping` or `ErrStopped` afterward. A subscriber created
during `Running` receives only events whose fan-out snapshot starts after insertion. A subscription created in `New` is
ready before the first event.

The worker locks the hub, checks `Running`, and performs only nonblocking sends to every current subscription. Because
sends are nonblocking and bounded, holding the lock cannot wait for a subscriber. `Unsubscribe` takes the same lock,
removes the subscription, and closes its channel. This single lock rule makes send-versus-close safe:

- The worker is the only sender.
- `Unsubscribe` or final hub cleanup closes, but only while holding the same mutex used for fan-out.
- After `Unsubscribe` returns, no future send can occur.
- Values already buffered remain readable before the closed state is observed.

Final worker cleanup takes the lock, transitions to `Stopped`, removes and closes every subscription, unlocks, and then
closes `done`. Public methods never wait while holding the mutex.

`Events` always returns the same receive-only channel. `Dropped` is available after unsubscribe and after hub shutdown.

### Lifecycle and concurrent subscription behavior

The baseline lifecycle applies. Starting with zero subscribers is valid: events are mapped and discarded until a
subscriber exists. This is useful for optional observers and avoids coupling listener lifetime to subscription count.

Concurrent operations have exact linearization points under the hub mutex:

- `Subscribe` linearizes when its entry is inserted.
- `Unsubscribe` linearizes when its entry is removed and channel closed.
- `Start` linearizes when state becomes `Running` after registration.
- `Stop` linearizes when state becomes `Stopping` and the stop request closes.
- Fan-out membership is the set present while the worker holds the mutex for that event.

Calling `Subscribe` concurrently with stop either inserts before `Stopping` and is subsequently closed by cleanup, or
fails with `ErrStopping`; it never returns an orphaned open channel. Unsubscribe may be called concurrently with fan-out
and stop without send-on-closed-channel panics.

### Delivery, backpressure, and ordering

Every subscriber present at an event's fan-out point is considered independently:

- `DropNewest`: if its buffer is full, discard the new event and increment `Dropped`.
- `DropOldest`: if full, nonblockingly remove one queued event, increment `Dropped`, then enqueue the new event.
  `Buffer` must be at least 1.
- `CancelSubscriber`: if full, remove the subscription, increment `Dropped`, and close its channel.

An unbuffered subscription is permitted only with `DropNewest` or `CancelSubscriber`. It receives an event only if a
receiver is ready at fan-out time. A positive buffer is the practical default.

The hub preserves source order for events successfully enqueued to each subscription. Different subscribers may observe
different subsets because their overflow states differ. All subscribers are processed by one fan-out worker, but
iteration order is deliberately unspecified and unobservable because sends do not block.

Duplicate source notifications are independently fanned out, subject to upstream coalescing and per-subscriber drops.
Shutdown discards source signals not yet fanned out. Events already in a subscription buffer remain drainable after its
channel closes.

Blocking fan-out is deliberately excluded. Supporting it while preserving unsubscribe safety would either let one
subscriber stop the hub or require per-subscriber goroutines and another queue layer. Returning a delivery error
synchronously is also impossible because signal receipt is asynchronous; `Dropped` is the bounded observability
mechanism.

### Error and validation model

Hub construction and lifecycle use common errors. `Subscribe` rejects:

- a negative buffer,
- an unknown overflow enum,
- `DropOldest` with a zero buffer.

These return `ConfigError`. `Unsubscribe` has no error because idempotency is more useful for deferred cleanup. Channel
closure means either explicit unsubscribe, overflow cancellation, or hub stop; callers that need the reason can add a
small terminal-reason accessor, but it should be omitted unless a concrete requirement exists.

### Testing strategy

- Verify every active subscriber receives the same event when capacity exists.
- Test subscribe-before-start and subscribe-during-run linearization with a controllable fake source.
- Stress concurrent `Subscribe`, `Unsubscribe`, fan-out, and `Stop`.
- Fill individual buffers and verify all three overflow policies, exact drop counts, and isolation of fast subscribers.
- Verify per-subscriber ordering after `DropNewest` and the expected retained suffix after `DropOldest`.
- Assert unsubscribe closes once, permits draining, and guarantees no later send.
- Verify final cleanup closes every live subscription and calls source `Stop` exactly once.
- Run high-iteration tests under `go test -race`.
- Keep real-signal tests in subprocesses and use platform build tags.

### Pros

- Dynamic broadcast with independent bounded backpressure.
- A slow subscriber cannot block the hub or another subscriber.
- No per-subscriber goroutines, which bounds resource use.
- Precise unsubscribe and channel-close ownership rules.
- Drop counts expose overload without an unbounded error path.

### Cons

- More API and internal synchronization than the other variants.
- Best-effort delivery; subscribers can observe different event sets.
- Nonblocking fan-out rules out guaranteed delivery.
- Mutable `T` values are shared among subscribers.

### Risks

- Consumers may overlook drop counters and assume broadcast is guaranteed.
- `DropOldest` must be implemented with one producer under the hub mutex; adding another producer later would invalidate
  the proof.
- A very large subscriber count makes each signal cost O(n) while holding the hub mutex.
- Long-lived abandoned subscriptions retain buffers until the hub stops, though they do not retain goroutines.

### Usage limitations

- Not suitable when every subscriber must acknowledge every event.
- Not durable, replayable, or cross-process.
- Blocking or transactional delivery requires a different system, not another option on this hub.
- For very large subscriber populations or expensive event cloning, an application-level event bus may be more
  appropriate.

## Comparative analysis

| Variant                  | Delivery audience                   | Backpressure                      | Ordering                                 | Dynamic listeners | Goroutines | Best fit                               |
|--------------------------|-------------------------------------|-----------------------------------|------------------------------------------|-------------------|------------|----------------------------------------|
| 1. Channel listener      | One competing consumer              | Blocking, bounded buffer          | Source order                             | No                | 1          | Simple event loops                     |
| 2. Callback dispatcher   | All fixed handlers                  | Slowest handler blocks all        | Source order, then handler order         | Before start only | 1          | Command-style lifecycle hooks          |
| 3. Context pull receiver | One explicit receiver               | Blocking, bounded internal queue  | Source order                             | No                | 1          | Context-centric structured concurrency |
| 4. Pub/sub hub           | All active subscribers, best effort | Per-subscriber drop or disconnect | Per-subscriber order for retained events | Yes               | 1          | Highly concurrent broadcast            |

## Recommendation

Variant 1, the single-consumer channel listener, is the recommended default. It has the smallest API, the clearest
ownership model, one worker, strong ordering, and an idiomatic fit with Go `select`. Most applications should receive a
signal-derived event in one coordinator and let that coordinator invoke application behavior.

Do not use Variant 1 when multiple independent components must each observe events or when a slow consumer must be
isolated. Use Variant 4 for highly concurrent pub/sub workloads and accept its explicit best-effort semantics.

Variant 1 is also the best simple-case architecture. Variant 3 is preferable only when an organization requires context
on every blocking operation or needs receive errors rather than channel closure. Variant 2 is appropriate for a small,
fixed, trusted set of context-aware handlers, but it should not be the package default because arbitrary callbacks make
shutdown and latency dependent on user code.

## Implementation guidance for the recommended variant

An implementation can proceed without changing its contracts:

1. Add build-tagged Unix signal validators and common error types.
2. Implement mapping validation and shallow copy in `NewListener`.
3. Store lifecycle state, stop ownership, and channel references under one mutex.
4. In `Start`, validate the state, transition to `Starting`, allocate channels, synchronously register the copied signal
   set, launch exactly one worker, publish `Running`, and return the receive-only channel.
5. In the worker, use cancellation-aware event sends. On stop, call source `Stop`, discard source-buffered
   notifications, close the event channel, publish `Stopped`, and close `done`.
6. In `Stop`, close the stop request once without holding the mutex while waiting. Treat caller context expiration as a
   wait failure, not a cleanup rollback.
7. Prove every channel has one closer and every blocking worker operation selects the stop path.
8. Add fake-source lifecycle and race tests before subprocess signal tests.

Exported identifier comments should state behavior rather than repeat names. Package names should remain short and
lower-case; APIs should avoid names such as `SignalListener` inside package `sigevent` when `Listener` is unambiguous.
Interfaces should stay private and one-purpose. No reflection, `unsafe`, global registries, or hidden non-stoppable
goroutines are needed.

## Platform and process notes

- Use `//go:build` files for supported Unix signal sets and corresponding tests.
- Reject uncatchable signals at construction.
- Test real delivery only in subprocesses so the test runner's own signal handlers are not disturbed.
- `signal.Stop` is mandatory for the exact registration channel. Do not close that channel and do not use process-wide
  `signal.Reset`.
- Signal semantics differ across Unix systems. A signal constant available on one target may not compile or behave
  identically on another; both validator code and integration tests must be target-specific.
- Standard signal delivery is not durable. If lossless commands are required, use IPC, a queue, or another acknowledged
  transport.

## Execution metrics

Reliable phase timing and token accounting were not exposed to the analysis environment. They are omitted rather than
estimated. The work was performed in default execution mode; no separate planning-mode interval was available.

## Final checklist

- Concurrency safety: one stated owner for mutable state and every channel close; concurrent lifecycle and subscription
  behavior is defined.
- Lifecycle: `New`, `Starting`, `Running`, `Stopping`, and `Stopped` transitions and method validity are explicit.
- Cancellation: stop channels and contexts unblock every package-controlled wait.
- Signal cleanup: the worker calls source-specific `Stop`; no global reset is used.
- Mapping validation: nil, empty, duplicate, unsupported, uncatchable, unmapped, external mutation, and many-to-one
  cases are covered.
- Delivery semantics: audience, duplicates, shutdown buffering, loss, and order are specified per variant.
- Backpressure: blocking, bounded buffering, drop-newest, drop-oldest, disconnect, and slow-listener isolation are
  compared.
- Testing: fake sources, lifecycle races, slow consumers, cancellation, subscriber churn, race detection, subprocess
  integration, and platform constraints are covered.
- Errors: sentinel errors plus narrow contextual wrappers are defined.
- Platform notes: build-tagged Unix validation and signal limitations are explicit.
- Style alignment: APIs are small, names avoid stutter, contexts lead blocking operations, channel ownership is
  explicit, and unnecessary exported abstractions and options are avoided.
