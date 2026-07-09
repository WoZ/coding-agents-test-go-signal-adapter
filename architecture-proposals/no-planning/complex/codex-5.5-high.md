# Architecture Proposals: Go OS Signal Adapter with Event Remapping

## Scope and assumptions

This document proposes architectures for a Unix Go 1.26 package named `signalmap`.
It was produced without using repository contents as design input.

The proposals make these assumptions:

- A listener is an explicitly constructed, single-use object. A successful `Start`
  permanently consumes its ability to start, even after it stops.
- A failed start caused by invalid configuration or failure to register the signal
  source does not consume the single successful start; the caller may correct a
  transient source failure and retry where the API permits it.
- `T` is any user-defined event type. The package neither compares nor mutates
  values of `T`.
- Mapping several signals to the same event is valid.
- Unix signal delivery is inherently lossy: the operating system and `os/signal`
  may coalesce repeated instances of the same signal. "Guaranteed delivery" below
  only concerns notifications already accepted by this package.
- Catching `SIGKILL` and `SIGSTOP` is impossible. Other signal support is
  platform-specific. The implementation cannot prove that every arbitrary
  `os.Signal` implementation is supported, so it rejects known-invalid values and
  documents that unknown values accepted by `signal.Notify` may simply never
  arrive; `signal.Notify` itself reports no registration error.
- The package controls only signal registration that it creates. Other process
  signal registrations remain outside its ownership.
- Examples use `map[os.Signal]T`. Construction defensively copies the map.
- Exact runtime and token accounting are not available to the generated document.

## Common contracts

### Mapping and construction

The preferred constructor shape is:

```go
func New[T any](mapping map[os.Signal]T, opts ...Option) (*Listener[T], error)
```

Options should exist only for material behavior such as buffer capacity or a drop
policy. Defaults must be documented and useful; avoid options for internal tuning.
The zero value of `Listener` is intentionally invalid because safe use requires a
validated, privately owned mapping and initialized lifecycle state.

Construction performs all configuration validation:

- A nil or empty mapping returns `ErrEmptyMapping`.
- A nil signal key returns a validation error wrapping `ErrInvalidSignal`.
- Known uncatchable signals, including Unix `SIGKILL` and `SIGSTOP`, return an
  error wrapping `ErrUnsupportedSignal`.
- Platform-specific signal checks belong in build-tagged files. Signals whose
  support cannot be determined statically are passed to source registration.
- Map-based configuration cannot contain duplicate keys. A slice-based alternative
  must explicitly detect duplicates and return an indexed validation error.
- Unmapped notifications are ignored defensively. Normal operation registers only
  mapped signals, so they should not occur.
- Multiple signals may map to the same event. `T` need not be comparable.
- The mapping and signal list are copied before publication. User mutation after
  construction cannot race with dispatch.

Prefer a small set of sentinel errors:

```go
var (
ErrEmptyMapping = errors.New("signalmap: empty mapping")
ErrInvalidSignal = errors.New("signalmap: invalid signal")
ErrUnsupportedSignal = errors.New("signalmap: unsupported signal")
ErrAlreadyRunning = errors.New("signalmap: start in progress or running")
ErrAlreadyStarted = errors.New("signalmap: listener already stopped")
ErrNotStarted = errors.New("signalmap: listener not started")
ErrStopped = errors.New("signalmap: listener stopped")
ErrInvalidBuffer = errors.New("signalmap: invalid buffer capacity")
ErrNilContext = errors.New("signalmap: nil context")
ErrNilHandler = errors.New("signalmap: nil handler")
ErrHandlerNotFound = errors.New("signalmap: handler not found")
)
```

Errors needing signal, subscriber, or handler context should wrap a sentinel with
`fmt.Errorf`. Do not create an error hierarchy. A constructor error is preferable
to a later asynchronous configuration error.

### Signal source and test seam

Production uses `signal.Notify` through an internal abstraction:

```go
type signalSource interface {
start([]os.Signal) (<-chan os.Signal, func (), error)
}
```

The production `start` allocates the channel, calls `signal.Notify`, and returns an
idempotent cleanup function that calls `signal.Stop` before relinquishing the
channel. The adapter, not `os/signal`, owns any wrapper channel it exposes. It must
never close a channel supplied to `signal.Notify`; cleanup first unregisters it,
then the adapter lets it become unreachable.

The error result supports fakes and possible alternative sources. The production
`os/signal` source returns nil after constructor-time validation because
`signal.Notify` has no error result.

An unexported constructor accepts `signalSource` for package tests. This keeps the
public API independent of `os/signal` mechanics while allowing deterministic
injection. If external source injection is a real product requirement, expose one
minimal `Source` interface in a separate advanced constructor; do not include it
preemptively.

The context variant may implement its production source with
`signal.NotifyContext`. Its returned `stop` function must always be called, even
when the parent context is already canceled.

### Shared lifecycle state machine

All variants use the same terminal lifecycle:

```text
New -> Starting -> Running -> Stopping -> Stopped
          |           |
          +-> New     +-> Stopping
          (failure)   (Stop or context cancellation)
```

`state`, `stopRequested`, source cleanup, and published run handles are protected by
one mutex. A `done` channel is created before state becomes `Starting` and is closed
exactly once by the run goroutine (or startup cleanup path) when state becomes
`Stopped`. Channels exposed to callers are closed only by their owning dispatcher.
No sender closes a channel.

Method rules:

| Method/state       | New             | Starting            | Running             | Stopping            | Stopped                       |
|--------------------|-----------------|---------------------|---------------------|---------------------|-------------------------------|
| `Start`            | proceed         | `ErrAlreadyRunning` | `ErrAlreadyRunning` | `ErrAlreadyStarted` | `ErrAlreadyStarted`           |
| `Stop`             | `ErrNotStarted` | request stop, wait  | request stop, wait  | wait, return nil    | return nil                    |
| delivery operation | not active      | not active          | variant-specific    | fails/ends          | `ErrStopped` or closed result |

`Start` changes `New` to `Starting` under the mutex, then registers without holding
the mutex. On registration failure it returns to `New`, closes that attempt's
internal completion handle, and returns the wrapped source error. On success it
publishes the source and changes to `Running`. If `Stop` arrived during registration,
`Start` immediately invokes cleanup, enters `Stopping`, and waits for finalization.
This makes concurrent `Start`/`Stop` deterministic without holding a lock around
external code.

`Stop` is synchronous: after requesting cancellation it waits for all package-owned
goroutines and source cleanup. Calling it from a package callback would deadlock in
the serial callback design; that variant therefore supplies a nonblocking
`RequestStop` for handlers and documents this restriction. Cancellation caused by
the parent context follows the same cleanup path. Repeated `Stop` calls are safe.

The mutex establishes safe publication. Atomics are unnecessary. The dispatcher
owns delivery and shutdown sequencing; cleanup is guarded by `sync.Once`.

## Variant 1: Single-consumer event stream

### Description and intended use

One dispatcher maps signals and sends events to one receive-only channel. This is
the smallest general-purpose API and fits daemons whose main loop already selects
over context, work, and lifecycle events.

### Public API sketch

```go
type Stream[T any] struct { /* private */ }

func NewStream[T any](mapping map[os.Signal]T, capacity int) (*Stream[T], error)
func (s *Stream[T]) Start(ctx context.Context) (<-chan T, error)
func (s *Stream[T]) Stop() error
func (s *Stream[T]) Done() <-chan struct{}
```

`capacity` must be nonnegative. Zero selects unbuffered delivery. A negative value
returns an error wrapping `ErrInvalidBuffer`.

### Components and concurrency

The stream contains the copied mapping, lifecycle mutex/state, cancellation
function, source cleanup, internal signal channel, event channel, and `done`.
Exactly one run goroutine reads source notifications and context cancellation. It
alone sends to and closes the event channel. `Start` safely publishes the receive-
only channel after source registration succeeds.

The send is cancellation-aware:

```go
select {
case events <- event:
case <-runCtx.Done():
}
```

Thus a blocked receiver cannot prevent `Stop` from completing. No per-event
goroutine is created.

### Delivery, backpressure, and ordering

- Each accepted mapped notification is offered to the one channel.
- Delivery blocks when the buffer is full; this gives lossless package-level
  delivery while running, subject to cancellation and upstream OS coalescing.
- A slow consumer backpressures signal intake. This can cause upstream
  notifications to be coalesced or dropped by `os/signal`.
- Events preserve the order in which the source channel supplies notifications.
- Duplicate accepted notifications produce duplicate events.
- On shutdown, a send that has already completed remains readable. Pending events
  in the channel buffer are retained and can be drained after closure. The event
  currently blocked on delivery is abandoned when cancellation wins.
- Only the run goroutine closes `events`, after source cleanup and after it stops
  sending. `Done` closes after `events`.

### Errors and validation

Construction uses the common validation. `Start` returns lifecycle or source
registration errors synchronously. Runtime OS notifications do not produce errors.
Context cancellation is observable through channel closure and `ctx.Err()`; it is
not sent on a second error channel. A nil start context returns `ErrNilContext`.

### Testing

Use a fake source with a test-owned input channel and cleanup counter. Verify exact
mapping and ordering, duplicate delivery, empty/unbuffered/full-buffer behavior,
draining after close, and cancellation of a blocked send. Table-test constructor
validation. Race concurrent `Start`/`Stop` calls and assert one successful start,
one cleanup, one event-channel close, and no goroutine remaining. A small
build-tagged Unix integration test may signal a helper subprocess; never send
process-wide signals to a parallel unit-test process.

### Pros

- Small, idiomatic API that composes directly with `select`.
- One goroutine and one clear channel owner.
- Strong ordering and simple leak-free cancellation.
- Easy deterministic testing.

### Cons

- Only one logical consumer; multiple readers compete rather than broadcast.
- Blocking backpressure can amplify OS-level signal loss.
- The caller must run an event loop.

### Risks

- Users may incorrectly start multiple channel readers expecting fan-out.
- A consumer that neither drains nor cancels will intentionally stall delivery.
- A very large capacity can retain large `T` values until drained.

### Usage limitations

Do not use for broadcast, subscriber isolation, or handlers that need independent
delivery policies. Package-level guarantees cannot recover signals coalesced before
the source reports them.

## Variant 2: Serial callback registry

### Description and intended use

The listener invokes registered handlers from its dispatcher goroutine. This suits
small applications where reacting to a signal is naturally expressed as a short
function and callers do not want to own an event loop.

### Public API sketch

```go
type Handler[T any] func (context.Context, T)
type HandlerID uint64

type Callbacks[T any] struct { /* private */ }

func NewCallbacks[T any](mapping map[os.Signal]T) (*Callbacks[T], error)
func (c *Callbacks[T]) AddHandler(Handler[T]) (HandlerID, error)
func (c *Callbacks[T]) RemoveHandler(HandlerID) error
func (c *Callbacks[T]) Start(ctx context.Context) error
func (c *Callbacks[T]) RequestStop()
func (c *Callbacks[T]) Stop() error
func (c *Callbacks[T]) Done() <-chan struct{}
```

### Components and concurrency

A mutex protects lifecycle state and a `map[HandlerID]Handler[T]`. IDs are
monotonic and never reused. On each event, the dispatcher copies handlers to a
snapshot under the mutex, unlocks, then invokes the snapshot serially in ascending
ID order. User code is never called while a package lock is held.

`AddHandler` is valid in `New`, `Starting`, and `Running`; nil handlers are rejected.
`RemoveHandler` is valid until `Stopping`. Removing a handler prevents inclusion in
future snapshots but does not cancel an invocation already in a snapshot.
Concurrent addition/removal is therefore race-free with explicit semantics.

### Delivery, backpressure, and ordering

- Every handler in the event's snapshot receives the event once.
- Events and handlers are invoked serially, preserving source order globally.
- Delivery is best-effort at shutdown: the current callback may finish, but events
  not yet dispatched are discarded.
- A slow or stuck handler blocks all subsequent handlers, signal reads, and normal
  shutdown. Handler contexts are canceled on stop, but Go cannot forcibly stop a
  handler that ignores cancellation.
- `RequestStop` only cancels and is safe inside a handler. `Stop` waits and must not
  be called by a handler executing on the dispatcher.
- Panics should not silently kill cleanup. Wrap each call with a narrow recovery
  boundary and report it through an optional, synchronous `OnPanic` policy, or
  document process-panic behavior. The recommended default is recovery plus a
  required package logger only if the surrounding product already has such a
  policy; otherwise let the panic propagate after deferred cleanup.

No event or handler goroutine is spawned. This deliberately chooses bounded
resource use over slow-handler isolation.

### Errors and validation

In addition to common errors, use `ErrNilHandler` and `ErrHandlerNotFound`.
`AddHandler` after stopping returns `ErrStopped`. `RemoveHandler` reports unknown
IDs. `Start` returns after the run goroutine is active; runtime handler failures
cannot be represented unless the handler owns their reporting.

### Testing

Test deterministic handler order, snapshot semantics during add/remove, removal of
unknown IDs, and reentrant registry changes from a handler. Verify context
cancellation, `RequestStop` from a handler, panic cleanup policy, and that a blocked
cooperative handler exits on cancellation. Stress concurrent registry mutations,
start, and stop under `go test -race`. Use time-bounded leak checks.

### Pros

- Convenient for direct reactions and dependency-injected application methods.
- Fan-out without exposing channels.
- Deterministic global ordering and bounded goroutine count.

### Cons

- User code runs on the critical dispatch path.
- One slow handler delays every listener.
- Handler errors have no natural return path.
- `Stop` has a callback-context restriction.

### Risks

- A handler that ignores context can leak the sole dispatcher and block `Stop`.
- Panic policy can surprise users if not made explicit.
- Long callbacks increase upstream signal loss.

### Usage limitations

Handlers must be short, nonblocking, cancellation-aware, and must not call `Stop`.
Use the pub/sub variant when subscribers need isolation or independent buffering.

## Variant 3: Context-oriented waiter

### Description and intended use

The listener exposes no event channel. Callers synchronously wait for the next
mapped event with a context. This fits sequential control flow, command runners,
and code that wants deadlines around each wait.

### Public API sketch

```go
type Waiter[T any] struct { /* private */ }

func NewWaiter[T any](mapping map[os.Signal]T) (*Waiter[T], error)
func (w *Waiter[T]) Start(ctx context.Context) error
func (w *Waiter[T]) Wait(ctx context.Context) (T, error)
func (w *Waiter[T]) Stop() error
func (w *Waiter[T]) Done() <-chan struct{}
```

### Components and concurrency

Production may use one `signal.NotifyContext` per mapped signal, but that introduces
multiple goroutines and loses a well-defined cross-signal order. The preferred
implementation uses one `signal.Notify` source behind the common abstraction and
uses contexts for lifetime and waits. If strict use of `NotifyContext` is required,
create one registration for all mapped signals only as a cancellation trigger; it
cannot identify which signal fired and therefore cannot perform arbitrary
remapping. That makes it unsuitable as the sole primitive for this contract.

One dispatcher maps source notifications into a bounded internal queue of capacity
one. `Wait` receives from that queue with:

```go
select {
case event, ok := <-queue:
case <-ctx.Done():
case <-w.Done():
}
```

Only the dispatcher closes the queue. Any number of concurrent `Wait` calls is
race-safe, but they are competing consumers: each event goes to at most one waiter.
The API should recommend one active waiter to keep assignment deterministic.

### Delivery, backpressure, and ordering

- Capacity one permits a signal to arrive just before `Wait`.
- Dispatcher delivery blocks while the slot is occupied, with stop cancellation in
  the send select.
- With one waiter, accepted events preserve source order. With concurrent waiters,
  event-to-waiter assignment is scheduler-dependent, though each event is consumed
  once.
- A canceled wait does not consume an event unless its receive wins the select.
  Go select tie-breaking means that if cancellation and an event are both ready,
  either result is allowed and documented.
- Shutdown closes the queue. A buffered event remains consumable; once drained,
  `Wait` returns zero `T` and `ErrStopped`.
- Parent cancellation stops the whole listener. Per-call cancellation affects only
  that `Wait`.

### Errors and validation

`Wait` before a successful start returns `ErrNotStarted`; during startup it may
return `ErrNotStarted` rather than wait. After draining shutdown data it returns
`ErrStopped`. A call-context error is returned directly so `errors.Is` works.
`Start(nil)` is impossible because context is an interface; explicitly reject a
nil context with `ErrNilContext` rather than panic.

### Testing

Test wait deadlines, already-canceled contexts, cancellation/event races within the
documented nondeterminism, one-slot buffering, multiple competing waiters, parent
cancellation, and buffered drain after shutdown. Fake source tests assert cleanup.
Stress `Wait`/`Stop` and run the race detector. Separately test an optional
`NotifyContext` source implementation and verify every returned stop function runs.

### Pros

- Natural deadline and cancellation support on each operation.
- No public channel ownership questions.
- Straight-line API for one-event-at-a-time workflows.

### Cons

- Less natural in a larger `select` loop.
- Concurrent waiters compete and assignment is nondeterministic.
- An internal queue still exists despite not being exposed.

### Risks

- Users may confuse per-wait cancellation with listener cancellation.
- Select races can return either an event or context error when both are ready.
- Calling `signal.NotifyContext` alone cannot identify the mapped signal.

### Usage limitations

This is not broadcast and does not provide strict waiter affinity. It is best with
one active waiter. Do not use it when the caller must select directly over multiple
channels or when every observer must receive every event.

## Variant 4: Isolated pub/sub broadcaster

### Description and intended use

A broker broadcasts each mapped event to all current subscribers. Every
subscription owns a bounded channel and an explicit overflow policy, isolating slow
subscribers. This is the appropriate design for highly concurrent in-process
fan-out.

### Public API sketch

```go
type OverflowPolicy uint8

const (
DropNewest OverflowPolicy = iota
DropOldest
DisconnectSlow
)

type Subscription[T any] struct { /* private */ }

func (s *Subscription[T]) Events() <-chan T
func (s *Subscription[T]) Dropped() uint64
func (s *Subscription[T]) Close() error

type Broker[T any] struct { /* private */ }

func NewBroker[T any](mapping map[os.Signal]T) (*Broker[T], error)
func (b *Broker[T]) Subscribe(capacity int, policy OverflowPolicy) (*Subscription[T], error)
func (b *Broker[T]) Start(ctx context.Context) error
func (b *Broker[T]) Stop() error
func (b *Broker[T]) Done() <-chan struct{}
```

### Components and concurrency

The broker run goroutine exclusively owns the live subscriber map and all sends and
closes. `Subscribe` and `Subscription.Close` send commands containing reply
channels to the broker. Before start, a mutex-protected pending list accepts
subscriptions; `Start` transfers it to the broker before publishing `Running`.
During stopping, new subscriptions return `ErrStopped`.

Each subscription channel has exactly one sender and closer: the broker. `Close`
does not close its own event channel; it submits removal and waits for broker
acknowledgment. Concurrent closes are idempotent. To prevent `Close` from hanging
during shutdown, command submission selects on broker `Done`, and shutdown replies
to or invalidates every accepted command before exiting.

Closing a pending subscription before `Start` removes it under the lifecycle mutex
and closes its channel there; no broker command is sent. Ownership transfers to the
broker atomically during `Start`, after which only the broker may close it.

No subscriber goroutines are created. The broker performs only nonblocking sends,
so subscriber behavior cannot block source intake or shutdown. Drop counters use
atomics because subscribers may read them while the broker increments them.

### Delivery, backpressure, and ordering

- Each event is offered once to every subscription present in the broker's
  snapshot at dispatch time.
- Per-subscriber source order is preserved among retained events.
- Delivery is best-effort. Full channels apply their configured bounded policy:
  `DropNewest` retains queued data; `DropOldest` removes one queued value then
  attempts the new send; `DisconnectSlow` removes and closes the subscription.
- A zero-capacity subscription cannot support nonblocking delivery and is rejected.
- Drop-oldest receive/send operations are safe because the broker is the only
  channel sender, but the consumer can race to free space. Both operations must be
  nonblocking; failure to remove means retry the send once, otherwise count a drop.
- Duplicate accepted source notifications are independently broadcast.
- On broker shutdown, queued subscriber events remain readable, then each channel
  reports closed. The broker alone closes every subscription channel.
- Subscribe/unsubscribe ordering is defined by broker command processing. A
  successful subscribe acknowledgment means subsequent broker-processed events are
  eligible. A successful close acknowledgment means no later sends can occur.

Returning delivery errors per event would create another backpressure stream and is
not recommended. `Dropped` provides observable aggregate loss. Products needing
audit-grade delivery need a durable queue, not an OS-signal adapter.

### Errors and validation

Construction uses common mapping validation. Subscribe rejects nonpositive
capacity and unknown policies. Closing an already closed subscription returns nil.
Operations on a subscription belonging to a stopped broker return `ErrStopped`
where a return value exists; `Events` simply reaches channel closure.

### Testing

Test all overflow policies with tiny buffers, subscriber isolation, exact retained
order, drop counters, subscribe/close acknowledgment boundaries, close during
shutdown, and buffered draining. Stress concurrent subscribe, unsubscribe, start,
and stop. Assert the broker is the sole closer and run under `go test -race`.
Property tests can generate event and command sequences and compare them with a
small broker model. Use fake-source cleanup assertions and helper-process Unix
integration tests.

### Pros

- True fan-out with bounded memory and slow-subscriber isolation.
- Explicit, observable overload behavior.
- Precise channel ownership and subscription boundaries.
- No goroutine per subscriber.

### Cons

- Largest API and most complex state coordination.
- Best-effort delivery requires callers to understand loss.
- Broker command traffic adds latency and implementation burden.

### Risks

- Incorrect command/shutdown arbitration can strand a caller waiting for an
  acknowledgment.
- Drop-oldest code is subtle when the consumer receives concurrently.
- Large subscriber counts multiply per-event work and retained memory.

### Usage limitations

Delivery is in-process, ephemeral, and `O(subscribers)` per event. It provides no
durability, replay, cross-process transport, or global ordering across separate
brokers. Do not use unbounded buffers.

## Comparison and recommendation

| Variant                | Delivery                 | Slow-consumer policy           | Ordering                       | Complexity | Best fit                          |
|------------------------|--------------------------|--------------------------------|--------------------------------|------------|-----------------------------------|
| Single-consumer stream | One channel              | Blocking, bounded              | Total source order             | Low        | General daemon/event loop         |
| Serial callbacks       | All handler snapshots    | Handler blocks dispatch        | Total source and handler order | Medium     | Short direct reactions            |
| Context waiter         | One competing waiter     | One-slot blocking queue        | Total with one waiter          | Low-medium | Sequential/deadline-oriented flow |
| Pub/sub broker         | Every current subscriber | Per-subscriber drop/disconnect | Per-subscriber retained order  | High       | Concurrent fan-out                |

**Recommended default: the single-consumer stream.** It has the smallest useful
surface, composes with idiomatic Go `select`, has one unambiguous channel owner, and
provides the strongest ordering and simplest leak-free shutdown proof. Its blocking
bounded delivery is easy to reason about, and users who need fan-out can add it at
the application boundary without forcing every package user to pay broker
complexity.

Do not choose the default when all observers must independently receive an event,
when blocking signal intake is unacceptable, or when callbacks are the established
application integration mechanism. The context waiter is best for the simplest
sequential cases that need one event and a deadline. The pub/sub broker is best for
highly concurrent fan-out with independently slow consumers. The callback variant
is ergonomic only when handlers are demonstrably short and cooperative.

## Implementation guidance

- Keep lifecycle coordination in one file and source implementations in small,
  build-tagged files where platform constants differ.
- Document every exported identifier and every blocking method. Avoid names such as
  `SignalListener` inside package `signalmap`; `signalmap.Stream` avoids stutter.
- Never hold the state mutex during source registration, channel operations, user
  callbacks, cleanup, or waiting on `Done`.
- Defer source cleanup and terminal state publication at the top of each dispatcher.
- Prefer one cancellation context per run over several ad hoc stop channels.
- Ensure all loop sends and command submissions select on cancellation.
- Keep interfaces consumer-defined and small. The internal source seam is enough
  until external injection is a demonstrated requirement.
- Run unit tests with `go test -race`, repeat lifecycle stress tests (`-count`), and
  use timeouts only as test failure bounds, not as synchronization.
- Gate real-signal tests by Unix build constraints and use subprocesses to avoid
  interfering with the Go test runner or parallel packages.

## Final checklist

- Concurrency safety: explicit mutex ownership, dispatcher ownership, publication,
  and concurrent operation semantics are defined.
- Lifecycle: single successful start and terminal stop state are defined.
- Cancellation: parent cancellation, explicit synchronous stop, and blocked-send
  escape paths are defined.
- Signal cleanup: every registration has an idempotent `signal.Stop` or
  `NotifyContext` stop path.
- Mapping validation: nil/empty, duplicates, unsupported signals, defensive copies,
  unmapped signals, and many-to-one values are covered.
- Delivery semantics: recipients, duplicates, shutdown draining, ordering, and
  loss boundaries are explicit.
- Backpressure: blocking, bounded buffering, drop-newest, drop-oldest, and
  disconnect behavior are addressed.
- Testing: fake sources, lifecycle races, cancellation, slow consumers,
  subscription mutation, race detection, and platform integration are covered.
- Errors: a small idiomatic sentinel set and wrapping strategy are defined.
- Platform notes: uncatchable and platform-specific Unix signals are covered.
- Style: minimal APIs, context-first methods, channel ownership, non-stuttering
  names, small interfaces, and no reflection, unsafe, or global mutable state are
  specified.

## Execution accounting

The work consisted of a prompt-analysis phase followed by document composition and
review in default execution mode. Exact phase timing and input/output/cache token
counts were not exposed to the document-generation environment, so no unverifiable
numbers are reported.
