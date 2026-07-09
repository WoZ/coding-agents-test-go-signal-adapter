# Architecture Proposals: Go OS Signal Adapter and Event Remapping

## Executive summary

This document proposes four architectures for a Unix Go package that converts
registered OS signals into caller-defined values:

1. a single-consumer event channel;
2. callback handlers;
3. a context-oriented, one-event `Wait` API;
4. an isolated multi-subscriber broker.

The single-consumer channel is the recommended default. It has the smallest
concurrent surface that still supports a long-running event stream, explicit
channel ownership, deterministic ordering, and lossless package-level delivery
with backpressure. The context-oriented variant is best when only the first
signal matters. The broker is best when many independent consumers must receive
every event and slow consumers must be isolated.

All variants use per-instance state, copy caller-owned configuration, permit
only one successful start attempt over an object's lifetime, and unregister
from `os/signal` on every terminal path.

## Common contracts

### Package and event model

The examples use package name `signalmap`. The name is short, does not stutter
with exported identifiers, and describes the package rather than an
implementation mechanism.

Events are generic:

```go
type Config[E any] struct {
	Mapping map[os.Signal]E
	Buffer  int
}
```

`New` copies `Mapping`; later caller mutation cannot race with delivery or
change behavior. `E` need not be comparable because it is only stored and
delivered. Different signals may intentionally map to the same event.

Map-based configuration cannot contain duplicate signal keys. A future
slice-based API would have to reject duplicates rather than silently use the
last value. Reflection, `unsafe`, package globals, and configuration options
without demonstrated value are avoided.

### Mapping validation

Construction fails when:

- the mapping is nil or empty;
- a signal key is nil;
- the configured buffer is negative;
- a variant-specific capacity is invalid.

The package registers only mapping keys, so an unmapped signal is not expected
from the production source. A fake source may produce one; it is ignored and
can be counted by optional internal test instrumentation. Multiple observations
of the same signal produce multiple mapped events if they reach the adapter.
Neither the package nor `os/signal` guarantees that the operating system will
queue every repetition; identical signals may be coalesced before the adapter
observes them.

There is no portable Go predicate for whether every arbitrary `os.Signal` is
supported by the current Unix kernel. The package validates shape, not kernel
semantics. Documentation and build-tagged integration tests list the supported
signals for each target. Uncatchable or semantically inappropriate signals,
such as `SIGKILL` and `SIGSTOP`, are documented as unsupported. Constructor
validation must not pretend that successful registration proves catchability.

### Lifecycle and errors

The shared logical state machine is:

```text
New -> Running -> Stopping -> Stopped
 |        |          |
 +--------+----------+----> Stopped (startup or source failure)
```

The start operation atomically consumes the object's only start opportunity.
Only `New` may transition into startup. No path returns to `New`, including a
failed source registration. A concurrent or later start returns
`ErrAlreadyStarted`.

Suggested errors are deliberately small:

```go
var (
	ErrAlreadyStarted = errors.New("signalmap: already started")
	ErrNotStarted     = errors.New("signalmap: not started")
	ErrStopped        = errors.New("signalmap: stopped")
	ErrClosed         = errors.New("signalmap: closed")
)
```

Invalid constructor input uses errors wrapping a stable
`ErrInvalidConfig` sentinel when callers need classification. Operational
source failures are wrapped with `%w`. Ordinary context termination remains
identifiable through `errors.Is(err, context.Canceled)` or
`context.DeadlineExceeded`.

`Stop` before start returns `ErrNotStarted`. The first call in `Running`
initiates shutdown and waits for completion. Concurrent and later calls after
startup are idempotent and return the same terminal result. This makes cleanup
safe in multiple owners without weakening the single-start rule.

### Signal source and cleanup

Production code hides `os/signal` behind a private, injected boundary:

```go
type signalSource interface {
	Subscribe(signals []os.Signal) (
		events <-chan os.Signal,
		cleanup func(),
		err error,
	)
}
```

The production implementation creates a private buffered signal channel, calls
`signal.Notify`, and returns a cleanup function that calls
`signal.Stop(channel)`. The cleanup function is idempotent. The adapter never
closes an `os/signal` channel: the component that sends owns that responsibility,
and `signal.Stop` does not close it.

The run goroutine defers cleanup immediately after successful subscription.
It also defers terminal state publication and closure of package-owned output
channels. Therefore cancellation, explicit stop, and internal failure all
unregister signal delivery exactly once.

Tests inject a fake source directly through an unexported constructor or a
package-internal factory. The abstraction is not exported until an external
source is a demonstrated user requirement.

## Variant 1: single-consumer event channel

### Description and intended use

One `Listener` maps signals and publishes a single ordered event stream. A
receive removes an event, so multiple goroutines reading the returned channel
compete rather than each receiving a copy.

This fits services with one lifecycle coordinator, select loops, and consumers
that want explicit backpressure.

### Public API sketch

```go
type Listener[E any] struct {
	// unexported fields
}

func New[E any](cfg Config[E]) (*Listener[E], error)

func (l *Listener[E]) Start(ctx context.Context) (<-chan E, error)
func (l *Listener[E]) Stop() error
func (l *Listener[E]) Done() <-chan struct{}
func (l *Listener[E]) Err() error
```

`Start` rejects a nil context. `Done` is available before start and closes only
after a consumed start attempt reaches `Stopped`. `Err` returns nil before
termination and afterward returns only an internal/source error; normal
explicit stop or context cancellation is not treated as a listener failure.

The zero value is not usable because valid mapping data is mandatory.

### Components and concurrency model

- A mutex protects lifecycle state, terminal error, cancellation function, and
  the one-time start decision.
- Immutable copied mapping data is safely published by `New`.
- `done` and the output channel are created by `New` and closed only by the run
  goroutine.
- One run goroutine is the sole output sender. It selects between the source
  channel, the derived run context, and delivery to the output channel.
- `Start` registers the source before publishing `Running`. Concurrent starts
  serialize under the mutex; exactly one consumes `New`.
- `Stop` copies the cancel function and `done` channel under the mutex, unlocks,
  invokes cancellation, and waits on `done`. It never waits while holding the
  mutex.

State publication and terminal error access occur under the mutex; channel
close provides the completion happens-before edge to waiters.

### Cancellation, delivery, and backpressure

For each observed mapped signal, the run loop performs:

```go
select {
case out <- event:
case <-runCtx.Done():
	return
}
```

Delivery is blocking after the bounded output buffer fills. No event observed
and accepted by the run loop is dropped by the package while the listener
continues running. Backpressure can delay reading the source, so the
`os/signal` input channel must also be bounded. It still cannot make signal
delivery globally lossless because the OS and `os/signal` can coalesce or drop
under sustained overload.

Events preserve the order in which the source channel yields signals. Duplicate
source observations yield duplicate events. On stop, a delivery currently
blocked on a slow consumer is abandoned through the cancellation arm. Events
already buffered in `out` remain readable after `out` is closed; unread source
signals are not drained. This gives finite, prompt shutdown.

The listener owns and closes `out` and `done`. Callers must never close them.

### Testing

- Validate all constructor failures and prove caller map mutation has no effect.
- Drive ordered and duplicate signals through a fake source.
- Fill the output buffer, stop without receiving, and prove prompt completion.
- Race many `Start` calls and assert one success and `ErrAlreadyStarted` for all
  others.
- Race repeated `Stop` calls before, during, and after startup.
- Cancel the parent context and verify cleanup, output closure, and `Done`.
- Inject registration and runtime failures and verify the terminal error.
- Run all concurrency tests under `go test -race`.
- In a subprocess-only Unix integration test, send a catchable signal to the
  child process. Never signal the main test process.

### Pros

- Small, idiomatic API that composes naturally with `select`.
- One sender and one closer make race and shutdown reasoning straightforward.
- Ordered, package-level lossless delivery has a clear backpressure contract.
- Few goroutines and no per-event allocation requirement.

### Cons

- It is not broadcast; multiple readers divide events.
- A slow consumer delays processing all subsequent signals.
- Buffer selection is an application-level capacity decision.

### Risks

- Users may incorrectly assume that Go or Unix queues every duplicate signal.
- A buffer that is too small increases upstream signal loss under bursts.
- A buffer that is too large defers backpressure and consumes unnecessary
  memory.

### Usage limitations

- Use a broker when every subscriber must see each event.
- Do not use blocking delivery where the sole consumer can stop reading without
  also canceling or stopping the listener.
- Delivery after shutdown is limited to events already in the output buffer.

## Variant 2: callback handlers

### Description and intended use

Users register context-aware handlers. The dispatcher invokes a stable snapshot
of current handlers for each event. This suits dependency injection and
applications whose lifecycle behavior is naturally expressed as methods rather
than channel select loops.

### Public API sketch

```go
type Handler[E any] func(context.Context, E)

type HandlerListener[E any] struct {
	// unexported fields
}

func NewHandlerListener[E any](cfg Config[E]) (*HandlerListener[E], error)

func (l *HandlerListener[E]) Handle(h Handler[E]) (remove func(), err error)
func (l *HandlerListener[E]) Start(ctx context.Context) error
func (l *HandlerListener[E]) Stop() error
func (l *HandlerListener[E]) Done() <-chan struct{}
func (l *HandlerListener[E]) Err() error
```

`Handle` rejects nil handlers. Its returned removal function is concurrency-safe
and idempotent.

### Components and concurrency model

- One mutex protects lifecycle state, terminal error, next handler ID, and the
  handler map.
- `Handle` is valid in `New` and `Running`; it returns `ErrStopped` in
  `Stopping` or `Stopped`.
- Removal is valid in all states. It deletes under the mutex and otherwise does
  nothing.
- For each event, the dispatcher copies handlers to a slice while holding the
  mutex, releases the mutex, then invokes the snapshot serially.
- Registration or removal concurrent with snapshot creation affects the next
  event. A handler already in a snapshot may run once after removal returns.
  This explicit rule avoids holding locks across user code.
- The dispatcher is the only package-created goroutine and owns `done`.

Serial invocation preserves source-event order and registration-snapshot order.
Handler IDs provide a deterministic invocation order; a Go map must not be
iterated directly for ordering.

### Cancellation, delivery, and backpressure

Handlers receive the run context. Stop cancels it and waits for the dispatcher.
No new event begins dispatch after cancellation is observed. If cancellation
occurs during a snapshot, remaining handlers are skipped and the current
handler is expected to return promptly after `ctx.Done()`.

Callbacks are synchronous and unbuffered. A slow handler applies backpressure
to every later handler and signal. The package does not create unbounded
goroutines to isolate callbacks. It recovers a panic around each call, converts
it into a terminal error containing the handler ID, cancels dispatch, performs
cleanup, and stops. Recovery prevents a user callback from crashing the
dispatcher silently; it does not conceal the failure.

Every source event is delivered once to each handler in the selected snapshot
unless cancellation or a handler panic terminates that dispatch. There is no
event replay for a newly registered handler.

### Testing

- Cover registration before and during running, idempotent removal, and the
  documented snapshot race.
- Verify deterministic handler order and serial event order.
- Block a context-aware handler, call `Stop`, then release it through context
  cancellation and verify no goroutine remains.
- Verify that panic becomes a terminal error and cleanup still runs.
- Race `Handle`, remove, `Start`, and `Stop` under the race detector.
- Reuse common fake-source, mapping, lifecycle, and subprocess tests.

### Pros

- Directly invokes application behavior without a consumer loop.
- Snapshotting avoids locks around user code.
- Dynamic registration is simple and deterministic.
- Serial execution has understandable ordering and bounded goroutine use.

### Cons

- User code executes on the signal dispatch path.
- A slow handler delays every handler and event.
- Removal cannot revoke a handler already captured in a dispatch snapshot.

### Risks

- A handler that ignores context and never returns can prevent `Stop` from
  completing. No Go API can forcibly terminate arbitrary user code safely.
- Panic policy and terminal error observability must be documented prominently.
- Callbacks may accidentally perform blocking or reentrant operations.

### Usage limitations

- Handlers must return promptly and honor cancellation; this is a required API
  contract for leak-free shutdown.
- Use pub/sub when subscribers need independent throughput.
- Callbacks are serial; callers needing parallel work must hand off into their
  own bounded, cancelable worker system.

## Variant 3: context-oriented one-event wait

### Description and intended use

`Wait` starts signal listening, blocks until the first mapped signal or context
termination, returns one event, and cleans up. This is the simplest design for
graceful shutdown or a one-shot control transition.

It is context-oriented because the operation lifetime and cancellation are
defined entirely by the caller's context. A direct `signal.NotifyContext`
implementation is appropriate only when every registered signal means the same
event: `NotifyContext` reports cancellation, not which signal occurred. The
general remapping implementation therefore uses the common signal source while
retaining context-first semantics.

### Public API sketch

```go
type Waiter[E any] struct {
	// unexported fields
}

func NewWaiter[E any](mapping map[os.Signal]E) (*Waiter[E], error)

func (w *Waiter[E]) Wait(ctx context.Context) (E, error)
func (w *Waiter[E]) Stop() error
func (w *Waiter[E]) Done() <-chan struct{}
```

`Wait` is the start method. It may be called successfully only once. Any
concurrent or later call returns the zero value and `ErrAlreadyStarted`.

### Components and concurrency model

`Wait` does not need a persistent delivery goroutine. Under a mutex it consumes
`New`, establishes a derived cancelable context, registers the source, and
publishes `Running`. It then selects synchronously over the source and context.
A deferred terminal function unregisters the source, transitions through
`Stopping` to `Stopped`, stores the result, and closes `done`.

`Stop` obtains and calls the cancellation function without holding the mutex,
then waits on `done`. State, cancellation, and the terminal result are
mutex-protected. Only the successful `Wait` invocation closes `done`.

### Cancellation, delivery, and backpressure

The first mapped signal returned by the source wins and is returned exactly
once. The waiter immediately unregisters; later signals are outside its
lifetime. There is no output channel, event buffer, shutdown drain, subscriber,
or package-level backpressure policy.

If signal readiness and context cancellation are simultaneous, Go `select`
chooses either ready case. The API explicitly permits either the event or the
context error in that race. This avoids implying timing priority that `select`
cannot guarantee.

An unmapped fake-source value is ignored and waiting continues. Source closure
without an event is an internal error. Context cancellation returns the zero
event and `ctx.Err()`.

### Testing

- Verify signal success, parent cancellation, deadline expiry, explicit stop,
  source closure, and registration failure.
- Race many `Wait` calls and prove exactly one owns the operation.
- Race `Stop` with signal delivery and accept only the two documented outcomes.
- Confirm cleanup and `done` closure on every terminal path.
- Test the all-signals-same-event `signal.NotifyContext` optimization
  separately if implemented; it is optional, not part of the required design.

### Pros

- Smallest public and internal surface.
- Natural context cancellation and no hidden long-lived delivery goroutine.
- Cleanup is scoped directly to one blocking operation.
- Ideal for command shutdown paths that need only one transition.

### Cons

- Returns only one event.
- Cannot support reload-style repeated events.
- The signal-versus-cancellation race has no strict priority.

### Risks

- Users may expect `signal.NotifyContext` to identify the triggering signal.
- Accidentally calling `Wait` twice is permanently rejected by design.
- A waiter created but never used has an open `done` channel, as documented.

### Usage limitations

- Not suitable for streams, handlers, or broadcast.
- Use one waiter per independent one-shot lifetime.
- Do not use it where every signal occurrence must be processed.

## Variant 4: isolated multi-subscriber broker

### Description and intended use

A broker broadcasts each mapped event to all active subscriptions. Each
subscription has a private bounded queue, so a slow subscriber does not block
the signal loop or other subscribers. This fits modular services with several
independent lifecycle components.

### Public API sketch

```go
type Subscription[E any] struct {
	C      <-chan E
	cancel func()
}

func (s *Subscription[E]) Cancel()

type BrokerConfig[E any] struct {
	Mapping            map[os.Signal]E
	SubscriberCapacity int
}

type Broker[E any] struct {
	// unexported fields
}

func NewBroker[E any](cfg BrokerConfig[E]) (*Broker[E], error)

func (b *Broker[E]) Subscribe() (*Subscription[E], error)
func (b *Broker[E]) Start(ctx context.Context) error
func (b *Broker[E]) Stop() error
func (b *Broker[E]) Done() <-chan struct{}
func (b *Broker[E]) Err() error
```

`SubscriberCapacity` must be positive because the selected isolation policy
requires a bounded queue.

### Components and concurrency model

- Before `Start`, a mutex protects state and a set of pending subscriptions.
- At successful start, the broker goroutine assumes exclusive ownership of the
  active subscription map and every subscription's send-side channel.
- During `Running`, `Subscribe` and `Cancel` send commands with acknowledgement
  channels to the broker. They select on both the command path and `Done`, so
  they cannot remain blocked during shutdown.
- `Subscribe` is valid in `New` and `Running`. In `New`, it creates a pending
  subscription under the mutex. In `Stopping` or `Stopped`, it returns
  `ErrClosed`.
- Cancellation is idempotent. Only the broker removes a running subscription
  and closes its event channel. Pending subscriptions are closed by the
  startup-failure or shutdown owner.
- The broker processes signal and subscription commands serially. No sender
  exists outside that goroutine after ownership transfer.

The handoff from mutex-owned pending state to goroutine-owned running state is
performed while startup holds the lifecycle mutex; `Running` is published only
after the broker has received the initial set. This prevents mixed ownership.

### Cancellation, delivery, and backpressure

Each received event is offered once to every subscription active at the
broker's dispatch point. Per-subscriber order matches source order.
Subscriptions created concurrently with an event receive it only if their add
command is processed first; canceled subscriptions may receive an event if its
cancel command is processed afterward.

The default slow-subscriber policy is bounded **drop oldest**:

1. try a nonblocking send;
2. if full, nonblockingly remove one queued event;
3. try the new event once more;
4. increment that subscription's internal dropped counter.

Only the broker sends, drops queued values, and closes. Concurrent subscriber
receives are valid channel operations. Drop-oldest favors current lifecycle
state and prevents one subscriber from applying global backpressure. Delivery
is therefore best-effort per subscriber, not guaranteed.

Alternative policies are consciously excluded from the initial API:

- blocking would defeat subscriber isolation;
- drop-newest preserves stale state;
- unbounded queues risk memory leaks;
- per-subscriber goroutines increase lifecycle complexity.

If callers need lossless broadcast, they need an external durable log or must
accept coordinated blocking; OS signals are not a durable transport.

On stop, no new signals are dispatched. The broker closes every subscription
channel without draining its buffers; Go channel semantics allow already
buffered events to be received after closure. It then unregisters the source
and closes `done`.

A small optional method such as `Dropped() uint64` may use an atomic counter,
but it should be exported only if observability is a product requirement.
Delivery errors are not returned synchronously because signal delivery has no
caller. Counts or metrics are the appropriate model.

### Testing

- Verify broadcast, per-subscriber ordering, and no replay to late subscribers.
- Fill one subscription, prove drop-oldest behavior, and prove another
  subscriber continues unaffected.
- Race subscription, cancellation, signal delivery, and stop.
- Verify every subscription channel closes exactly once on cancel, startup
  failure, and broker shutdown.
- Assert command senders unblock when shutdown wins.
- Run a high-volume stress test under the race detector and use bounded
  timeouts to detect leaked goroutines.
- Reuse common validation, lifecycle, cleanup, and subprocess tests.

### Pros

- True broadcast to independent consumers.
- Slow subscribers are isolated with bounded memory.
- One broker owns all send and close operations.
- Dynamic subscription has explicit, testable linearization points.

### Cons

- Most complex API and implementation.
- Events can be dropped for an overloaded subscriber.
- Command acknowledgement and ownership transfer require careful code.

### Risks

- Consumers may overlook best-effort semantics.
- Drop metrics can be necessary to diagnose undersized queues.
- Fairness among simultaneously ready signal and command channels is not
  strict; sustained load requires stress testing.

### Usage limitations

- Not appropriate when every event must be durably delivered.
- Capacity must reflect the slowest acceptable subscriber reaction time.
- Subscription cancellation is linearized by the broker, not by the instant
  `Cancel` begins.

## Comparative analysis

| Variant | Consumer model | Package delivery | Ordering | Slow consumer effect | Best use |
|---|---|---|---|---|---|
| Channel | One competing stream | No package drops while running | Source order | Backpressures the listener | General default and select loops |
| Callbacks | All registered handlers | All snapshot handlers unless canceled or panic | Source and handler order | Delays all handlers | Method-oriented applications |
| Context wait | One event to one caller | First observed event | Not applicable beyond first | None after return | Simple shutdown/one-shot control |
| Pub/sub broker | All active subscribers | Best-effort, drop oldest per full queue | Per-subscriber source order | Isolated to that subscriber | Highly concurrent modular services |

## Recommendation

Use the single-consumer channel architecture by default. It exposes Go's native
coordination primitive, has one clear sender/closer, minimizes shared mutable
state, and states backpressure rather than hiding it. It is the easiest
long-running design to implement and verify without sacrificing cancellation
or event ordering.

Do not use it when independent components must each observe the same event; use
the pub/sub broker. Do not use it when user behavior must be registered as
callbacks and serial blocking is acceptable; use the handler variant. For the
common simple case where one termination signal ends a command, use the context
waiter because it has no streaming machinery.

No variant can make Unix signals durable. Applications requiring guaranteed,
replayable, or cross-process event delivery should use a durable messaging
system rather than expanding this package.

## Cross-variant implementation and test guidance

Exported identifiers require doc comments that state lifecycle and concurrency
contracts. Interfaces remain private and one-method. Constructors validate all
mandatory state, so zero values are intentionally invalid. Context is the first
parameter of blocking start/wait operations and is never stored beyond the
operation lifetime. Errors are wrapped, not string-matched.

Mutexes protect only lifecycle metadata and registry snapshots; no design holds
a mutex while waiting, sending, closing, or calling user code. Atomics are
reserved for independent counters. Every goroutine has a cancellation path and
a completion signal. Channel closure has exactly one statically identifiable
owner.

The complete implementation test matrix should include:

- table-driven mapping and capacity validation;
- external mutation of the original mapping;
- all legal and illegal lifecycle transitions;
- concurrent start and stop, including startup failure;
- deterministic source cleanup exactly once;
- ordered and duplicate event observations;
- cancellation during blocked delivery or dispatch;
- subscriber/handler addition and removal at concurrency boundaries;
- slow-consumer behavior for the selected policy;
- terminal channel closure and buffered-event behavior;
- `go test -race ./...` with repeated stress runs;
- build-tagged or runtime-skipped subprocess integration tests per Unix target.

Integration tests must avoid `SIGKILL`, `SIGSTOP`, and signals used by the Go
runtime or test harness. Windows support is outside this proposal.

## Assumptions

- Target platforms are Unix systems supported by Go 1.26.
- User event values are safe for the caller's intended concurrent use. The
  package copies map entries but cannot deep-copy pointer-containing events.
- Signal mapping is immutable after construction.
- Exact preservation of every physical OS signal occurrence is not required,
  because the underlying facility cannot guarantee it.
- Production uses one private source channel per adapter instance; no global
  signal registry is introduced.
- Callback users accept the contract that handlers return and honor
  cancellation.
- The pub/sub default favors recent state through bounded drop-oldest queues.

## Execution metadata

- Execution mode: planning
- Model/effort filename: `codex-5.5-medium.md`
- Planning duration: not reliably exposed by the execution environment
- Input, output, and cache token usage: not reliably exposed by the execution
  environment

## Final checklist

- Concurrency safety: covered through explicit state and channel ownership.
- Lifecycle: single-start state machine and valid operations defined.
- Cancellation: parent context and idempotent `Stop` defined.
- Signal cleanup: `signal.Stop` abstraction and terminal cleanup defined.
- Mapping validation: nil, empty, mutation, duplicates, platform limits, and
  shared events covered.
- Delivery semantics: audience, duplication, ordering, and shutdown buffering
  defined per variant.
- Backpressure: blocking, serial, no-stream, and bounded drop-oldest models
  compared.
- Testing: fake source, lifecycle, concurrency, slow consumer, race, and Unix
  integration coverage defined.
- Errors: small sentinel set, wrapping, and context errors defined.
- Platform notes: Unix scope and uncatchable-signal limitations stated.
- Style alignment: idiomatic naming, small interfaces, context placement,
  channel ownership, and mutex discipline stated.
