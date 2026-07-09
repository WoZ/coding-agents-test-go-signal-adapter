# Architecture proposals for a Go OS-signal event adapter

## Executive summary

This document proposes four architectures for a Go 1.26 package that translates Unix OS signals into caller-defined event values. All variants hide `os/signal` registration, allow cancellation, enforce a one-start-per-instance lifecycle, and can be tested without sending real process signals.

The **single-consumer channel adapter** is the recommended default. It has the smallest API that still gives callers idiomatic cancellation, ordering, and backpressure control. The **bounded multi-subscriber broker** is the best option when the package itself must support highly concurrent pub/sub delivery. The callback dispatcher is useful for tightly encapsulated sequential handling, while the composable pipeline is appropriate when applications need replaceable dispatch policies or richer integration.

## Common contracts and assumptions

The following rules apply to every variant:

- A mapping is supplied at construction time as `map[os.Signal]E`, where `E` is any caller-defined event type. The constructor validates the signal keys and defensively copies the map. It does not retain caller-owned mutable data structures.
- `Start` is successful at most once per object. The first successful call permanently consumes the object's start capability, even after cancellation or natural termination. Every later call returns `ErrAlreadyStarted`; internal state is never reset.
- A context that is already canceled may still be passed to the first `Start`: the start succeeds, returns its normal delivery handles, and then terminates promptly. This keeps lifecycle behavior deterministic.
- `Stop` is safe for concurrent use and idempotent. Before `Start`, it is a no-op and does not consume the start capability. During or after a successful start, repeated calls return without error.
- The implementation owns all channel closure. Callers must never close package channels.
- A single coordinator goroutine receives raw signals and owns the running lifecycle. It selects on cancellation during every potentially blocking package operation. On exit it calls `signal.Stop` for its private raw-signal channel, releases references, closes output channels in their documented order, and closes `Done`.
- The raw signal channel is buffered. Its capacity is configurable within a documented safe range and defaults to at least the number of registered signals. This reduces bursts but cannot guarantee capture of every OS signal: Unix standard signals may be coalesced by the OS, and `os/signal` uses non-blocking delivery.
- Signal-to-event mappings are immutable after construction. Supporting live mapping changes would add synchronization and ambiguous ordering and is outside the proposed APIs.
- Multiple adapter instances may register for the same signal. Each instance receives independently according to `os/signal` semantics, which may be surprising to applications expecting competing consumers.
- Package internals use a small signal-registration interface implemented by `os/signal` in production and a deterministic fake in tests. This interface is not public and does not expose raw signal channels to package users.
- Examples assume package name `signaladapter`. Exact option names may be adjusted to the repository's naming conventions during implementation.

A shared error and lifecycle surface can be used by all variants:

```go
var ErrAlreadyStarted = errors.New("signal adapter has already been started")

type lifecycle struct {
    // A mutex protects the state transition and installation of cancel.
    // sync.Once protects cleanup and Done closure.
}
```

An atomic compare-and-swap can enforce the start transition, but a mutex is generally clearer because `Start` must also publish the cancellation function and signal registration atomically with respect to `Stop`. The state machine is `new -> running -> stopped`; there is no transition back to `new`.

## Variant 1: single-consumer channel adapter

### Description and API

The adapter exposes one receive-only event stream. One coordinator receives raw signals, looks them up in the immutable mapping, and sends mapped events in receive order. Unmapped signals are ignored defensively, although only mapped signals are registered.

```go
type Adapter[E any] struct {
    // unexported lifecycle, mapping, registrar, and options
}

func New[E any](mapping map[os.Signal]E, opts ...Option) (*Adapter[E], error)

func (a *Adapter[E]) Start(ctx context.Context) (<-chan E, error)
func (a *Adapter[E]) Stop()
func (a *Adapter[E]) Done() <-chan struct{}
```

The event channel has a configurable bounded capacity. The default is small and documented. When it fills, delivery blocks, but the coordinator's send always selects on cancellation:

```go
select {
case events <- event:
case <-runCtx.Done():
    return
}
```

On termination the coordinator first unregisters its raw channel, then closes the event channel, and finally closes `Done`. A consumer may therefore range over the event channel until closure and then inspect its own context for the shutdown cause.

### Advantages

- Small, idiomatic API based on contexts and receive-only channels.
- Preserves the order observed by the adapter.
- No callback reentrancy or package-managed handler concurrency.
- Straightforward deterministic tests with a fake registrar.
- The caller chooses whether to process sequentially, launch workers, or fan out.

### Disadvantages

- Only one logical stream is supplied; multiple goroutines reading it compete rather than each receiving every event.
- A slow or absent consumer applies backpressure to the coordinator.
- The package cannot report per-event handling failures because handling belongs to the caller.

### Risks

- An undersized output buffer can increase the chance that a burst is lost before `os/signal` can enqueue it.
- Callers may mistakenly treat multiple channel readers as broadcast subscribers.
- A caller that neither consumes events nor cancels retains the adapter goroutine by design.

### Usage limitations

- Not a built-in broadcast mechanism.
- Provides process-local observation, not durable or exactly-once messaging.
- Best when one component owns signal handling or performs its own downstream dispatch.

## Variant 2: serialized callback dispatcher

### Description and API

The caller provides a handler. The coordinator invokes it synchronously for each mapped event, preserving order and ensuring at most one handler invocation at a time. A handler error is terminal: it is published once and initiates cleanup.

```go
type Handler[E any] func(context.Context, E) error

type CallbackAdapter[E any] struct {
    // unexported fields
}

func NewCallback[E any](
    mapping map[os.Signal]E,
    handler Handler[E],
    opts ...Option,
) (*CallbackAdapter[E], error)

func (a *CallbackAdapter[E]) Start(ctx context.Context) (<-chan error, error)
func (a *CallbackAdapter[E]) Stop()
func (a *CallbackAdapter[E]) Done() <-chan struct{}
```

The returned error channel has capacity one and is closed on termination. It receives only a terminal handler error; context cancellation is observed through the caller's context. The package does not recover handler panics: silently converting arbitrary panics into errors would hide programmer faults and make process state uncertain.

The handler receives the run context and must honor its cancellation. Because Go cannot forcibly interrupt a function call, cleanup cannot complete until a running handler returns.

### Advantages

- Keeps translation and handling behind one controlled abstraction.
- Guarantees serialized, ordered handler calls without caller synchronization.
- Gives handler errors an explicit terminal path.
- Easy to test with a recording or blocking handler.

### Disadvantages

- A slow handler delays all later signal processing.
- Callback code executes on the adapter's lifecycle goroutine.
- Cancellation is cooperative while a handler is running.
- Less composable than a channel at call sites.

### Risks

- A handler that blocks forever causes a goroutine leak that the adapter cannot prevent.
- Handler code may call back into application components in unsafe lock order.
- An unhandled panic terminates the process unless recovered at a higher level.

### Usage limitations

- Appropriate only when event handlers are bounded and cancellation-aware.
- Not suitable for independent subscribers or parallel processing without adding concurrency inside the handler.
- Does not provide retry semantics; applications needing retries should use the pipeline variant with a dedicated dispatcher.

## Variant 3: bounded multi-subscriber broker

### Description and API

This variant broadcasts every mapped event to a dynamic set of subscriptions. Each subscription owns a bounded event channel and an overflow policy. Registry changes, sends, and channel closure are serialized through the coordinator, preventing send-versus-close races.

```go
type OverflowPolicy uint8

const (
    DropNewest OverflowPolicy = iota
    DropOldest
    Block
)

type Subscription[E any] interface {
    Events() <-chan E
    Dropped() uint64
    Close()
}

type Broker[E any] struct {
    // unexported fields
}

func NewBroker[E any](
    mapping map[os.Signal]E,
    opts ...BrokerOption,
) (*Broker[E], error)

func (b *Broker[E]) Subscribe(buffer int, policy OverflowPolicy) (Subscription[E], error)
func (b *Broker[E]) Start(ctx context.Context) error
func (b *Broker[E]) Stop()
func (b *Broker[E]) Done() <-chan struct{}
```

`Subscribe` is concurrency-safe before and after `Start`. A subscription remains valid until it is closed or the broker terminates. After broker termination, new subscriptions fail with `ErrStopped`. `Subscription.Close` is idempotent, waits until the coordinator has removed the subscription, and guarantees that `Events` is closed before it returns.

The default policy is `DropNewest`, which prevents one slow subscriber from stalling unrelated subscribers. `Dropped` is an atomic cumulative counter:

- `DropNewest`: discard the incoming event when the subscriber buffer is full.
- `DropOldest`: remove one queued event and enqueue the newest event. Only the coordinator sends or performs the eviction, so this does not race with channel closure.
- `Block`: wait for that subscriber, selecting on broker cancellation. This preserves adapter-observed delivery for active subscriptions but lets one subscriber delay all others.

Subscribers are visited in stable registration order. This does not create a timing guarantee across subscribers; it only makes behavior reproducible. No goroutine is created per subscriber.

### Advantages

- Native broadcast semantics with dynamic, independent subscribers.
- Bounded memory and explicit slow-consumer behavior.
- No per-subscriber goroutine, keeping resource use predictable.
- Drop counters make overload observable.
- Subscription closure and broker shutdown have precise synchronization guarantees.

### Disadvantages

- Considerably larger API and state machine.
- Dropping policies weaken delivery guarantees; blocking weakens subscriber isolation.
- The coordinator performs work proportional to subscriber count for every event.
- Dynamic subscribe/unsubscribe requires command and acknowledgement plumbing.

### Risks

- Selecting `Block` for an untrusted subscriber can stall the broker.
- Large subscriber counts or buffers can consume significant CPU and memory.
- Users may fail to monitor drop counts and silently miss operational events.
- `DropOldest` makes an overloaded subscriber observe gaps that are not obvious from event values alone.

### Usage limitations

- Not durable, replayable, or distributed pub/sub.
- Ordering is preserved within each subscription only for events actually retained.
- Subscription count and buffer sizes should be bounded operationally.
- Use only when broadcast belongs inside this package; otherwise keep the default adapter and use an existing application message bus.

## Variant 4: composable mapper and dispatcher pipeline

### Description and API

This design separates signal registration, mapping, and event dispatch. The production signal source remains internal, while mapping and dispatch are public extension points. A pipeline coordinator invokes the mapper and then the dispatcher.

```go
type Mapper[E any] func(os.Signal) (E, bool)

type Dispatcher[E any] interface {
    Dispatch(context.Context, E) error
    Close(context.Context) error
}

type Pipeline[E any] struct {
    // unexported source and lifecycle
}

func NewPipeline[E any](
    signals []os.Signal,
    mapper Mapper[E],
    dispatcher Dispatcher[E],
    opts ...PipelineOption,
) (*Pipeline[E], error)

func (p *Pipeline[E]) Start(ctx context.Context) (<-chan error, error)
func (p *Pipeline[E]) Stop()
func (p *Pipeline[E]) Done() <-chan struct{}
```

A convenience constructor converts an immutable map into a `Mapper`. The explicit signal list is necessary for custom mappers because `os/signal` registration must know which signals to request.

`Dispatch` calls are serialized by the coordinator unless the supplied dispatcher implements its own concurrency. A dispatch error is terminal and is sent on the capacity-one error channel. During cleanup, signal registration is stopped before `Dispatcher.Close` is called with a configurable bounded shutdown context. The error channel reports the first dispatch or close error and is then closed.

Tests construct the pipeline through an unexported constructor that accepts a fake signal source. Public users can replace mapping and downstream delivery without gaining access to raw `os/signal` mechanics.

### Advantages

- Clean separation between acquisition, translation, and delivery.
- Supports queues, worker pools, telemetry wrappers, batching, or application buses behind `Dispatcher`.
- Mapping behavior can be unit-tested independently.
- Dependency inversion keeps the OS-specific mechanism internal.

### Disadvantages

- More interfaces, configuration, and lifecycle composition than most callers need.
- Correct shutdown depends partly on third-party dispatcher implementations.
- A custom mapper can be stateful or unsafe even though the package invokes it serially.

### Risks

- A dispatcher that ignores context can block shutdown and leak resources.
- Incorrectly configured signal lists can disagree with a custom mapper.
- Layering may obscure which component owns retry, buffering, and error policy.
- `Close` failures require a clear operational reporting path.

### Usage limitations

- Intended for integration-heavy systems, not minimal signal handling.
- The package guarantees serialized calls into the dispatcher, but cannot guarantee the dispatcher's internal race safety.
- Exactly-once delivery remains impossible without a durable external dispatcher and application-level idempotency.

## Comparative analysis

| Variant | API complexity | Delivery model | Backpressure | Ordering | Fan-out | Best fit |
|---|---:|---|---|---|---|---|
| Channel adapter | Low | One event stream | Bounded blocking channel | Preserved on the stream | Caller-managed | General default and simple cases |
| Callback dispatcher | Low–medium | One serialized handler | Handler duration | Preserved | Handler-managed | Small applications with bounded handlers |
| Subscriber broker | High | Broadcast subscriptions | Per-subscription drop or block policy | Per retained subscription stream | Built in | Highly concurrent pub/sub |
| Composable pipeline | Medium–high | Pluggable dispatcher | Dispatcher-defined | Serialized at interface boundary | Dispatcher-defined | Complex integration and custom delivery |

## Recommendation

Use the **single-consumer channel adapter** by default. It is idiomatic Go, has the smallest concurrency surface, keeps policy in the application, and makes ownership and cancellation easy to understand. It is also the best variant for simpler cases.

Do not use it when the package itself must guarantee broadcast semantics to independently managed listeners, or when downstream delivery requires standardized retries, batching, or instrumentation. Use the **bounded multi-subscriber broker** for highly concurrent in-process pub/sub, with `DropNewest` as the safe default and monitored drop counters. Use the **callback dispatcher** only for small, fast, cancellation-aware sequential handlers. Use the **composable pipeline** when dispatch is an architectural boundary and its additional interfaces are justified.

## Test strategy and acceptance criteria

All variants should run unit tests under `go test -race`, with repeated stress runs for lifecycle tests.

Common test cases:

- The first `Start` succeeds and every sequential or concurrent later call returns `ErrAlreadyStarted`.
- Cancellation before and immediately after `Start` terminates cleanly.
- Concurrent `Start`, `Stop`, and context cancellation do not race, panic, double-close channels, or leak goroutines.
- `Stop` before `Start` is harmless; repeated `Stop` calls are harmless.
- The constructor's defensive copy is unaffected by later caller mutation.
- Mapped signals produce the exact caller-supplied values; unregistered or unmapped signals do not.
- Cleanup always unregisters the raw signal channel and closes outputs in the documented order.
- A blocked output send remains cancelable.
- Fake-source closure and injected terminal conditions do not spin the coordinator.
- Goroutine counts return to baseline within a bounded test deadline after shutdown.

Variant-specific cases:

- Channel adapter: bounded-buffer behavior, order preservation, and range-until-close.
- Callback adapter: serialization, terminal handler errors, blocking-handler cancellation limitations, and error-channel closure.
- Broker: concurrent subscribe/unsubscribe, all overflow policies, exact drop counters, no send-after-close, subscriber ordering, and shutdown with full buffers.
- Pipeline: mapper filtering, dispatcher error propagation, close invocation exactly once, bounded close context, and dispatcher wrappers.

Tests should use fake signals rather than process-wide `syscall.Kill` except for a small Unix integration test. Any real-signal test must use a signal safe for the test process, isolate global signal registration, call cleanup, and avoid parallel execution.

## Execution metadata

- Execution mode: planning
- Model/effort filename: `codex-5.5-high.md`
- Planning and implementation timing: unavailable from the execution environment with sufficient accuracy
- Input, output, and cache token usage: not exposed by the execution environment

