# OS signal-to-event adapter architecture proposals

## Scope and assumptions

These proposals target Go 1.26 on Unix. The package name used below is `signalevent`. User event type `E` is generic and
may be any type; the adapter does not compare event values. The caller supplies concrete `os.Signal` values, normally
`syscall.Signal` constants available on the target Unix platform.

Common assumptions:

- A process has relatively few configured signals and signal bursts are uncommon, but overload behavior must still be
  explicit.
- One adapter object owns one registration with the process-wide `os/signal` facility. Applications must coordinate
  adapters that register overlapping signals because `os/signal` broadcasts to all registered channels.
- “Start once” means one successful transition out of `New` during the object's lifetime. A failed `Start` caused by
  invalid state does not reset the object. Constructor validation happens before an object is returned.
- `Stop` is idempotent and safe concurrently with all documented operations. Cancellation and `Stop` have the same
  terminal effect.
- An OS signal is only a notification; Unix may coalesce repeated identical signals before Go receives them. Therefore
  no architecture can guarantee one event per signal sent by the OS.
- Multiple signals may map to the same user event. Each notification received from the signal source still causes a
  separate delivery attempt.
- Generic APIs are shown. A project that cannot use generics can replace `E` with `any` without changing the
  architecture.
- `Start` does not accept an already-canceled context as a successful start. It returns `context.Canceled` (or the
  context cause) and leaves the object in `New`, because no listening began.

## Common contracts and implementation rules

### Mapping and source boundary

The public constructor accepts `map[os.Signal]E`, validates it, and copies it. Neither the map nor its key slice is
subsequently exposed. A map cannot represent duplicate keys; a slice-based alternative would reject duplicate signals
explicitly rather than silently taking the last value.

```go
var (
ErrAlreadyStarted = errors.New("signal listener already started")
ErrNotStarted = errors.New("signal listener not started")
ErrStopped        = errors.New("signal listener stopped")
ErrClosed = errors.New("subscription closed")
ErrInvalidMapping = errors.New("invalid signal mapping")
)

type Source interface {
Notify(chan<- os.Signal, ...os.Signal)
Stop(chan<- os.Signal)
}
```

`Source` is an internal interface. Production code adapts `signal.Notify` and `signal.Stop`; tests inject a fake. The
interface is deliberately small and does not expose `Source` publicly unless a test-only/internal constructor is
impractical. An unexported `newWithSource` can be tested from package tests. `Notify` must be called before the run loop
starts. `Stop` must be called exactly once after successful registration, before the source channel is discarded.

The constructor rejects:

- a nil or empty map;
- a nil signal key;
- optionally, a nil event when `E` is an interface or pointer only if the package contract chooses to forbid it. The
  recommended contract permits zero and nil event values because generic nil detection would require reflection and user
  events are opaque;
- a configured signal that the selected source explicitly cannot support, if a platform-specific validator is supplied.

There is no portable runtime API that proves every arbitrary `os.Signal` is supported by `os/signal`. The baseline
validates shape, not OS semantics. Platform constraints should be enforced by Unix build-tagged convenience helpers or
by documenting accepted `syscall.Signal` values. `SIGKILL` and `SIGSTOP` cannot be caught and should be rejected where
identifiable. Platform-specific signal constants belong in caller files with appropriate build tags.

Unmapped signals cannot normally enter because only mapped keys are registered. A fake or faulty source may send one;
the dispatcher ignores it and may increment an optional internal/observability counter. It must never synthesize a zero
event.

### Lifecycle

All variants use this irreversible state machine:

```text
                    Start succeeds
New ----------------------------------------------> Running
 |                                                     |
 | Stop                                                | Stop or context cancellation
 v                                                     v
Stopped <------------------------------------------ Stopping
```

`Stopping` is an internal transient state. There is no transition back to `New` or `Running`.

- `Start` is valid only in `New`. The first successful call atomically claims the object. Concurrent losers return
  `ErrAlreadyStarted`.
- `Stop` in `New` moves directly to `Stopped`, permanently preventing start. `Stop` in `Running` initiates shutdown. In
  `Stopping` or `Stopped`, it waits for or observes completion and returns nil.
- A context cancellation races safely with `Stop`; exactly one shutdown path performs cleanup.
- Delivery-specific registration methods define their own valid states below.

A mutex protects lifecycle state, the cancel function, and mutable subscriber metadata. It is never held while invoking
user code or performing a potentially blocking channel send. A `done` channel is created by the constructor, closed
exactly once by the run goroutine after cleanup, or by `Stop` when stopping from `New`. `sync.Once` may guard
cancellation initiation and terminal closure, but does not replace the lifecycle mutex. All fields needed by the run
goroutine are fully initialized before it is launched.

The source channel has a bounded buffer, normally configurable with a conservative default such as 16. Only the source
sends it. The adapter never closes it: closing a channel still registered with `os/signal` can panic when the package
sends. Shutdown order is:

1. mark `Stopping` and cancel the run context;
2. run loop observes cancellation and stops accepting new work;
3. call `Source.Stop(sourceCh)` to unregister;
4. apply the variant's documented buffered-event policy;
5. close adapter-owned output/subscription channels where applicable;
6. mark `Stopped` and close `done`.

The production source adapter should make cleanup idempotent internally or be called from the sole run goroutine.
`Stop(ctx)` can use a context to bound the caller's wait, but timeout does not abandon cleanup; it returns `ctx.Err()`
while internal shutdown continues. A simpler `Stop()` blocks until complete. API sketches below use
`Stop(context.Context) error`.

Errors use a small set of sentinel errors so callers can use `errors.Is`. Constructor errors wrap `ErrInvalidMapping`
with the offending field or signal. User callback failures are ordinary `error` values routed through a documented hook
or result stream; they do not mutate lifecycle unless policy says so. Panics are discussed per variant.

## Variant 1: single-consumer event channel

### Description and intended use

One run goroutine maps source notifications and sends events to one receive-only channel. This is the smallest
general-purpose API and the recommended default for an application component with one owner, an event loop, or a
`select` over signals and other inputs.

### Public API sketch

```go
type Listener[E any] struct { /* unexported */ }

func New[E any](mapping map[os.Signal]E, opts ...Option) (*Listener[E], error)

// Start begins listening exactly once and returns the adapter-owned event stream.
func (l *Listener[E]) Start(ctx context.Context) (<-chan E, error)

// Stop initiates shutdown and waits for completion or ctx cancellation.
func (l *Listener[E]) Stop(ctx context.Context) error

// Done is closed after signal registration and output cleanup complete.
func (l *Listener[E]) Done() <-chan struct{}
```

Options should remain few: source-buffer capacity, event-buffer capacity, and overflow policy. Prefer explicit
constructors if options grow. The zero value is not usable because a validated mapping is mandatory.

### Components and concurrency model

The listener contains the copied mapping, stable signal-key slice, internal `Source`, source channel, output channel,
run context/cancel function, lifecycle mutex/state, and `done`. Exactly one run goroutine owns mapping reads, output
sends, output closure, source unregistration, and final state publication. Mapping is immutable, so reads need no lock.

Concurrent `Start` calls are serialized by the lifecycle mutex. The winner installs cancellation state and starts the
goroutine; other calls return `ErrAlreadyStarted` and a nil channel. `Stop` copies required references under lock,
releases it, cancels, and waits. There are no subscribe operations.

### Delivery, ordering, and backpressure

Each notification received and found in the mapping produces at most one send to the single stream. Multiple goroutines
may receive from the returned channel, but then each event goes to exactly one of them nondeterministically; this is
work sharing, not broadcast.

The run goroutine preserves the order in which notifications are read from the source channel. This is not necessarily
kernel send order across different signal types. Duplicate notifications read by the adapter remain duplicates.

Recommended default: a bounded event buffer and **blocking delivery**, with cancellation included in the send:

```go
select {
case out <- event:
case <-ctx.Done():
return
}
```

This gives no adapter-level drops while running, but a slow consumer backpressures the run loop. Meanwhile the bounded
source channel or OS may coalesce/drop notifications. Alternative explicit policies are `DropNewest` (nonblocking send
with a drop counter/hook) and `DropOldest` (adapter owns the output channel and may remove one buffered item before
retrying). `DropOldest` is safe only because the run goroutine is the sole sender, though receivers race legitimately
for queued values. Unbounded buffering is not offered.

On shutdown the recommended policy discards notifications not yet mapped and closes `out` after any already-sent
buffered events; receivers can drain those values before observing closure. A configurable “drain source” mode is
discouraged because signals can keep arriving around unregistration and prolong shutdown. A successfully selected send
is considered delivered even if the consumer has not processed it.

Only the run goroutine closes `out`; callers must never close it. If `Stop` occurs before `Start`, an output channel
created at construction is closed by the same terminal helper, and no channel was returned. `Done` is closed by the
terminal helper exactly once.

### Validation and errors

Common mapping validation applies. `Start` returns `ErrAlreadyStarted` after any prior successful start, and also after
`Stop` has made start permanently impossible (it may wrap both `ErrAlreadyStarted` and `ErrStopped`, but one stable
sentinel is preferable). `Stop` is nil on repeated calls. Invalid options fail construction. Delivery drops are
operational events, not synchronous errors; expose an optional nonblocking observer callback or counters, avoiding an
errors channel that itself needs backpressure semantics.

### Testing

Use a fake source that records `Notify`/`Stop` and permits deterministic sends. Test mapping and order, duplicate
delivery, ignored unmapped input, every overflow policy, blocked-send cancellation, output drain/closure,
stop-before-start, concurrent starts with exactly one success, and many concurrent stops. Verify that fake `Stop` occurs
once and before completion. Run stress tests under `go test -race`; add a Unix integration test that sends a catchable
signal to the test process, isolated because signals are process-global and skipped in unsuitable CI environments.

### Pros

- Smallest API and implementation; clear channel ownership.
- Natural composition with `select` and context cancellation.
- Strong per-stream ordering and straightforward leak prevention.
- Best default when one component owns signal handling.

### Cons

- Not broadcast; multiple receivers compete.
- Blocking policy lets a slow consumer delay mapping.
- Drop policies require observability to avoid silent loss.
- Caller must run an event loop.

### Risks

- A consumer that neither drains nor cancels can stall until `Stop`; cancellation-aware sends prevent permanent
  goroutine leakage once stopped.
- Users may incorrectly assume one event per kernel signal.
- Two listener objects registering the same signal both receive it.

### Usage limitations

Do not use when every subscriber must see every event, handlers require isolation, or processing must run concurrently.
It cannot provide end-to-end guaranteed delivery because OS and `os/signal` delivery are lossy/coalescing.

## Variant 2: callback/handler runner

### Description and intended use

The adapter invokes a registered handler for each mapped event. This suits applications that want inversion of control,
centralized error handling, and do not need dynamic subscriptions.

### Public API sketch

```go
type Handler[E any] interface {
HandleSignalEvent(context.Context, E) error
}

type HandlerFunc[E any] func (context.Context, E) error

func (f HandlerFunc[E]) HandleSignalEvent(ctx context.Context, e E) error {
return f(ctx, e)
}

type Runner[E any] struct { /* unexported */ }

func NewRunner[E any](
mapping map[os.Signal]E,
handler Handler[E],
opts ...RunnerOption,
) (*Runner[E], error)

func (r *Runner[E]) Start(ctx context.Context) error
func (r *Runner[E]) Stop(ctx context.Context) error
func (r *Runner[E]) Done() <-chan struct{}
```

The handler is mandatory and immutable. This avoids concurrent registration complexity; a separate pub/sub variant
handles that requirement.

### Components and concurrency model

In the simplest and recommended mode, one run goroutine reads, maps, and invokes the handler synchronously. It never
holds the state mutex during invocation. Handler calls are serialized and receive the run context. The user must make
the handler return when that context is canceled.

An optional bounded worker-pool mode uses one dispatcher plus a fixed number of workers and a bounded job channel. The
dispatcher alone closes the job channel; workers exit on context cancellation or closure; a `WaitGroup` completes before
`done` closes. This mode must be explicitly enabled because it weakens completion ordering and makes shutdown more
complex. It must never start one unbounded goroutine per event.

### Lifecycle, cancellation, and cleanup

The common lifecycle applies. `Start` returns after registration and goroutine launch, not after completion. `Stop`
cancels the context, unregisters source delivery in the dispatcher, and waits for the handler/worker set subject to the
caller's stop context.

Synchronous handlers cannot be forcibly terminated. If a handler ignores cancellation forever, complete shutdown cannot
be guaranteed; this is an unavoidable Go limitation and a documented usage constraint. Worker-pool mode has the same
constraint. Returning early from `Stop` on timeout does not close `done` or pretend cleanup finished.

### Delivery, ordering, and backpressure

Synchronous mode attempts one handler call for every mapped notification read. Calls begin and complete in source-read
order, and there is never concurrent invocation. Backpressure is blocking: a slow handler stops source reads.

Worker-pool mode preserves enqueue order but not handler start/completion order. Different events may run concurrently.
Its bounded queue supports explicit policies:

- `Block` (default): no adapter-level drop; cancellation interrupts enqueue.
- `DropNewest`: discard the incoming job and report via the error/observation hook.
- `DropOldest`: dispatcher removes the oldest queued job before adding the new one.

“Return delivery error” cannot be returned from `Start`, which has already completed. Instead `OnError(func(error))` may
observe handler and overflow errors. The hook must be fast, panic-free, and is invoked serially by the dispatcher, or
observations go to a user-supplied nonblocking logger interface. The package should not own a hidden error channel
unless its closure and overflow policy are equally explicit.

Recommended shutdown drops jobs not yet started and cancels running handlers. An optional `Drain` mode stops source
registration, closes the job queue, and waits for queued handlers using a non-canceled processing context; it can exceed
deadlines and therefore should require the stop context as a bound. Synchronous mode has no adapter job buffer.

### Error and panic policy

Handler errors are passed to `OnError` and processing continues by default. An explicit `StopOnHandlerError` option may
cancel the runner, storing the first terminal error retrievable via `Wait()` if that method is added. Keep this policy
out unless required; otherwise `Done` plus observation is sufficient.

By default, do not recover handler panics: silently converting programmer bugs is surprising. If a server requires
process isolation, offer an explicit `RecoverPanics` option that converts the recovered value plus stack to an error
hook and defines whether processing continues. This still cannot repair corrupted user state.

### Validation and testing

Validate the common mapping, non-nil handler, positive worker count and queue capacity, and compatible policies.
Typed-nil handlers cannot be reliably rejected without reflection; document that they are caller errors and avoid
reflection solely for this check.

Tests use a recording handler and controllable blocking handler. Cover serialization, worker concurrency bound, ordering
modes, cancellation passed to handlers, each queue policy, handler errors, optional panic behavior, stop timeouts, and
no worker leaks. Lifecycle, fake-source, race, and Unix integration tests follow the common strategy.

### Pros

- Minimal caller plumbing and a natural place for processing errors.
- Serialized mode has simple ordering and synchronization.
- Bounded worker mode supports controlled concurrency.
- No event channel ownership burden for users.

### Cons

- User code runs inside adapter-managed goroutines.
- A non-cooperative handler can prevent shutdown completion.
- Worker mode weakens ordering and expands policy surface.
- Dynamic handlers are intentionally unsupported.

### Risks

- Handler reentrancy: calling `Stop` synchronously from the handler can deadlock if `Stop` waits for that same handler.
  The contract must forbid blocking `Stop` from a handler; the handler may cancel the parent context or call a
  nonblocking `RequestStop`.
- Error hooks can themselves block or panic.
- Worker concurrency can violate assumptions in a non-thread-safe handler.

### Usage limitations

Use synchronous mode only for bounded, cancellation-aware work. Use worker mode only when events are independent or
completion ordering is irrelevant. This is not suitable for dynamic fan-out.

## Variant 3: context-oriented signal scopes

### Description and intended use

Each mapped event is represented by cancellation of a dedicated context. This is built on `signal.NotifyContext` in
production and is optimized for one-shot lifecycle conditions such as “begin graceful shutdown.” It deliberately treats
events as state transitions, not a stream.

### Public API sketch

```go
type Scope[E comparable] struct { /* unexported */ }

func NewScope[E comparable](
parent context.Context,
mapping map[os.Signal]E,
) (*Scope[E], error)

// Start creates event contexts and registers signal notification exactly once.
func (s *Scope[E]) Start() (map[E]context.Context, error)
func (s *Scope[E]) Stop(context.Context) error
func (s *Scope[E]) Done() <-chan struct{}
```

Because contexts are indexed by event, `E` must be `comparable`. A clearer alternative for a small fixed set is
`ContextFor(event E) (context.Context, bool)` after `Start`, avoiding publication of a mutable map. Returned contexts
are values, and the returned map is a copy.

### Components and registration model

Invert the mapping into `event -> []os.Signal`. Multiple signals mapped to the same event share one context. During
`Start`, create one `signal.NotifyContext(parent, signals...)` per distinct event, retain both the context and its
mandatory stop function, and start one coordinator goroutine that waits for parent cancellation or explicit stop and
then invokes every stop function.

For deterministic unit tests, use an internal `ContextSource` abstraction:

```go
type contextSource interface {
NotifyContext(context.Context, ...os.Signal) (context.Context, context.CancelFunc)
}
```

The production implementation calls `signal.NotifyContext`. The returned stop functions unregister signal delivery and
restore the prior signal behavior after cancellation semantics permit. Every stop function is called exactly once during
aggregate shutdown, even if its context already became done.

### Lifecycle and concurrency

The common state machine applies. The lifecycle mutex protects state, the immutable-after-start context lookup,
aggregate cancel function, and completion setup. All event contexts are constructed before they are published, so
callers never observe partial registration. If registration construction cannot fail, publication is atomic under the
mutex.

`Stop` cancels the aggregate parent, invokes all retained signal stop functions through the coordinator, marks stopped,
and closes `done`. Concurrent `Start` calls have one winner. `ContextFor` returns `ErrNotStarted` in `New`, the stored
context in `Running`, and the same now-canceled context in `Stopping`/`Stopped`, allowing stable lookup after start.
There is no restart.

### Delivery and backpressure

Cancellation is broadcast: every goroutine waiting on the event context observes `Done`. The first matching signal
closes that context; later matching signals have no additional representation. There is no event payload channel, queue,
backpressure, ordering between event contexts, drop policy, or duplicate count. This is precisely why the variant is
safe and useful for state-like events but unsuitable for commands such as repeated reload requests.

If two distinct events' signals arrive close together, both contexts may be canceled with no specified order. On adapter
shutdown, all event contexts are canceled, so consumers must distinguish “specific signal occurred” from “scope stopped”
if needed. One solution is a small result object with an atomic cause set before cancellation, but context cancellation
causes would require the adapter to observe signals itself, undermining the direct `NotifyContext` design. The simple
contract therefore treats either signal or scope shutdown as cancellation only.

### Validation, errors, and testing

Common validation applies plus comparability at compile time. Duplicate event values are valid and merge their signal
lists. `Start` returns `ErrAlreadyStarted`; `ContextFor` reports unknown events with `(nil, false)` and lifecycle errors
separately if desired.

Unit tests inject fake derived contexts, verify atomic publication, one-shot broadcast, merged signal groups,
stop-function invocation, parent cancellation, stop-before-start, and concurrent lifecycle calls. A small Unix
integration test checks `signal.NotifyContext` behavior. The race detector must remain clean.

### Pros

- Excellent fit with existing context-aware servers and goroutines.
- Broadcast cancellation needs no buffers and cannot block.
- Very small delivery mechanism and simple leak-free shutdown.
- `signal.NotifyContext` cleanup is explicit through retained stop functions.

### Cons

- Loses repeated events, payload streaming, and ordering.
- `E` must be comparable in the sketched lookup API.
- Adapter shutdown is observationally the same as event occurrence.
- One underlying registration per distinct event may duplicate registrations if groups overlap due to a validation bug.

### Risks

- Callers may misuse context values as an event bus.
- Forgetting to call every returned stop function leaks process-level signal registrations.
- A parent canceled before `Start` must not be accepted as a successful lifetime start.

### Usage limitations

Use only for idempotent, one-shot state transitions. It is not appropriate for reload counters, audit streams, ordered
workflows, or any requirement to process every received notification. It is the best simple-case architecture for
graceful shutdown alone.

## Variant 4: dynamic pub/sub broker

### Description and intended use

A broker broadcasts every mapped event to all active subscribers, with a bounded queue and overflow policy per
subscription. It is intended for highly concurrent applications where components independently observe signals and slow
consumers must be isolated.

### Public API sketch

```go
type OverflowPolicy uint8

const (
DropNewest OverflowPolicy = iota
DropOldest
DisconnectSlow
)

type Subscription[E any] struct { /* unexported */ }

func (s *Subscription[E]) Events() <-chan E
func (s *Subscription[E]) Close() error
func (s *Subscription[E]) Dropped() uint64

type Broker[E any] struct { /* unexported */ }

func NewBroker[E any](mapping map[os.Signal]E, opts ...BrokerOption) (*Broker[E], error)
func (b *Broker[E]) Start(ctx context.Context) error
func (b *Broker[E]) Subscribe(buffer int, policy OverflowPolicy) (*Subscription[E], error)
func (b *Broker[E]) Stop(ctx context.Context) error
func (b *Broker[E]) Done() <-chan struct{}
```

Subscriptions are explicit owned handles; callers receive but never close `Events`.

### Components and concurrency model

The recommended implementation uses an actor-style broker goroutine as the sole owner of the subscriber map and sole
sender/closer of every subscription channel. Subscribe, unsubscribe, source-event, and stop requests enter
bounded/control channels. Each request includes a one-shot reply channel so the caller knows when the operation has
taken effect.

Lifecycle state used by `Start` and initial command-channel publication remains behind a mutex. After start, the actor
owns subscriber membership. `Subscription.Close` sends an unsubscribe command and waits for acknowledgment; concurrent
closes use `sync.Once` and return nil after the first. An atomic counter per subscription exposes dropped count without
taking the actor lock.

To prevent control-plane starvation under signal floods, the actor loop checks cancellation and drains a bounded number
of source events before rechecking control requests; alternatively use separate high-priority control selection. Command
sends always select on broker `done`, so Subscribe/Close cannot leak while shutdown races.

No per-subscriber forwarding goroutine is required: the actor performs only nonblocking sends to bounded subscription
channels. This isolates slow subscribers and bounds goroutine count. A blocking per-subscriber policy is intentionally
not offered because one slow subscriber would stall every subscriber. If guaranteed delivery is required, a durable
external message system—not an OS-signal adapter—is the correct architecture.

### Lifecycle and subscription rules

- `Subscribe` in `New` is allowed: the broker stores a pending subscription under the lifecycle mutex, and `Start`
  transfers the complete set to the actor before source processing. This supports race-free setup.
- `Subscribe` in `Running` sends an actor command and becomes active only after acknowledgment. It returns `ErrStopped`
  in `Stopping`/`Stopped`.
- `Subscription.Close` is valid in every state. Before start it removes/closes the pending subscription under the mutex.
  While running the actor removes and closes it. After broker stop it is already closed.
- `Start` and `Stop` follow the common once-only state machine. Stop-before-start closes all pending subscription
  channels.

The implementation must carefully serialize the handoff from pending subscribers to actor ownership under the lifecycle
mutex before publishing `Running`; this prevents `Close` from acting through the wrong ownership path.

### Delivery, ordering, and backpressure

For each mapped notification, the actor iterates a stable subscriber set and attempts one send to every subscriber
active when actor processing begins. Subscription changes acknowledged before an event is processed affect that event;
concurrent unacknowledged changes have no guarantee.

Each subscriber observes events in broker processing order, minus drops. Subscribers may observe different subsets.
Iteration order does not matter because sends are nonblocking. Duplicate source notifications cause duplicate attempts.
No ordering is promised between an event and a concurrent subscribe/unsubscribe request until its acknowledgment
establishes the boundary.

Policies:

- `DropNewest`: if full, retain queued events and count the new event as dropped.
- `DropOldest`: if full, the actor receives one value from the subscriber channel, increments the counter, then sends
  the new value. This is safe because the actor is the sole sender, although a subscriber may concurrently receive
  first; use a nonblocking receive and retry send to handle that race.
- `DisconnectSlow`: on full, remove the subscription and close its channel. Optionally record a terminal reason
  retrievable from the handle.

A zero buffer is permitted only with `DropNewest` or `DisconnectSlow`; it delivers only when a receiver is ready. Reject
`DropOldest` with zero capacity. Set an application-wide maximum buffer to prevent accidental excessive allocation.

On shutdown, the actor stops registration, stops accepting source events, and closes every subscription channel. Values
already buffered remain receivable before closure is observed. Source notifications not yet broadcast are discarded.
Only the actor closes running subscription channels; pending channels are closed by the lifecycle owner before actor
handoff.

### Errors and validation

Apply common mapping checks. Validate positive control-channel capacity internally, subscriber buffer bounds, and valid
policy values. `Subscribe` returns `ErrStopped` after shutdown; command cancellation due to shutdown must translate
consistently to `ErrStopped`. `Close` is idempotent. Drops are exposed as counters rather than returned errors because
broadcast is asynchronous. `DisconnectSlow` can expose `ErrClosed` plus a reason through a nonblocking `Err() error`
accessor after channel closure.

### Testing

Use a fake source and deterministic actor acknowledgments. Test broadcast to all subscribers, per-subscriber ordering,
subscribe/unsubscribe boundaries, close idempotence, pre-start subscriptions, stop-before-start, all overflow policies,
isolation of a stalled subscriber, drop counters, and thousands of concurrent subscribe/close operations. Verify the
actor and all command callers exit during stop races. Use goleak-style checks if the project accepts that dependency,
otherwise track internal completions explicitly. Run repeated stress tests under `go test -race`, plus isolated Unix
signal integration tests.

### Pros

- True broadcast with independently bounded subscribers.
- Slow consumers do not block healthy consumers.
- Actor ownership makes channel closing and membership changes precise.
- Best fit for highly concurrent in-process pub/sub.

### Cons

- Largest API and implementation.
- Delivery is necessarily best-effort for bounded isolation.
- Actor command protocol and pre-start handoff require careful tests.
- Per-subscriber buffers consume memory proportional to subscriber count.

### Risks

- An actor blocked on an improperly designed reply path can deadlock; reply channels should be capacity one.
- A subscriber that loses its handle cannot explicitly unsubscribe, though broker stop still cleans it up.
- Large subscriber counts make broadcast O(n) per notification.
- `DropOldest` logic is easy to implement incorrectly under concurrent receivers.

### Usage limitations

Not suitable when all subscribers require guaranteed processing, when subscriber count or buffers are unbounded, or when
events must survive process failure. Use a durable broker for those requirements.

## Comparison and recommendation

| Variant           | Recipients                                 | Default pressure policy         | Ordering                    | Dynamic fan-out  | Complexity | Best use                           |
|-------------------|--------------------------------------------|---------------------------------|-----------------------------|------------------|------------|------------------------------------|
| 1. Single channel | One consumer per event                     | Bounded, blocking               | Source-read order           | No               | Low        | General application event loop     |
| 2. Handler runner | One handler invocation                     | Blocking; optional worker queue | Strict when serial          | No               | Medium     | Centralized processing             |
| 3. Context scopes | All context waiters, once                  | None                            | None across contexts        | Waiters may join | Low        | Graceful shutdown/state transition |
| 4. Pub/sub broker | Every active subscriber, subject to policy | Per-subscriber drop/disconnect  | Per subscriber, minus drops | Yes              | High       | Concurrent component broadcast     |

**Recommended default: Variant 1, the single-consumer channel.** It has the smallest complete contract, composes
naturally with Go control flow, keeps user code out of internal goroutines, preserves an explicit order, and makes
cancellation-aware backpressure easy to reason about. Do not use it when several independent components must each
receive the same event or when processing callbacks must be managed by the adapter.

For the simplest one-shot graceful-shutdown case, choose Variant 3. Its cancellation broadcast matches the semantic
requirement and avoids inventing a stream. For highly concurrent pub/sub, choose Variant 4; its actor and per-subscriber
bounded policies provide explicit isolation. Choose Variant 2 when one central handler is already the application's
natural integration boundary, especially if its work is short and cancellation-aware.

## Cross-cutting implementation notes

- Document every exported identifier and avoid names such as `SignalListener` inside package `signalevent` when
  `Listener` is clear.
- Keep `Source`, state enums, and synchronization details unexported. Do not export an interface merely for mocking.
- Never copy a struct containing a mutex; expose constructors returning pointers.
- Do not hold a mutex across channel operations, source calls, callbacks, or waits.
- Make cancellation causes observable only when they add real value; ordinary sentinel errors and `context.Cause` are
  sufficient.
- Avoid package globals, reflection, `unsafe`, finalizers, and unbounded goroutine creation.
- Treat signal registration as process-global integration in documentation. Installing adapters can change default
  signal behavior while registered.
- Unix build tags are appropriate for integration tests and helpers using platform-specific constants. The core can use
  `os.Signal`.
- CI should run unit tests normally and under `go test -race`; real-process signal tests should not run in parallel and
  should restore registration reliably with `defer`.

## Execution metadata

The proposal was produced in default execution mode. Exact phase timing and token accounting (input, output, cached, and
reasoning tokens) were not exposed to the document-writing environment, so no reliable numerical breakdown is reported.

## Final checklist

- Concurrency safety: explicit ownership, mutex boundaries, actor ownership, safe publication, and cancellation-aware
  sends.
- Lifecycle: irreversible `New -> Running -> Stopping -> Stopped`, including stop-before-start and concurrent calls.
- Cancellation: context-driven and idempotent, with bounded caller waits.
- Signal cleanup: `signal.Stop` or every `NotifyContext` stop function is mandatory before completion.
- Mapping validation: nil/empty maps, nil signals, copied maps, platform limits, duplicates, and unmapped inputs
  addressed.
- Delivery semantics: recipient set, duplicates, ordering, shutdown buffers, and OS coalescing stated per variant.
- Backpressure: blocking, bounded buffers, drop-newest, drop-oldest, disconnect, and slow-subscriber isolation covered.
- Testing: fake source, lifecycle races, cancellation, slow consumers, subscriber churn, race detector, and Unix
  integration covered.
- Errors: small sentinel set, wrapped validation errors, and asynchronous error observation defined.
- Platform notes: Unix signals, uncatchable signals, build tags, and process-global registration covered.
- Style alignment: small interfaces, constructor validation, context-first cancellation, clear channel ownership, no
  reflection/unsafe, and minimal exported APIs.
