# Architecture Proposals: Go OS Signal Adapter and Event Remapping

## Assumptions

- The target is Unix and Go 1.26.
- A mapped event is any caller-defined Go value. Generic type parameter `E` is used so callers keep compile-time type safety.
- Each adapter instance has a permanent one-shot lifecycle: `Start` may succeed only once. After start, cancellation, completion, or a start-time error, that instance cannot be started again.
- Cancellation is safe to call concurrently and more than once.
- Signal delivery is process-global at the operating-system boundary. Tests therefore inject a signal source rather than sending real process signals except in isolated integration tests.
- Events derived from signals are notifications, not durable messages. Each variant documents its behavior when consumers cannot keep up.
- The illustrative APIs omit some constructors and validation details where they do not affect the architecture.

## Common Design Foundation

All variants should isolate `os/signal` behind an internal source:

```go
type signalSource interface {
	Notify(signals ...os.Signal) (<-chan os.Signal, func())
}
```

The production implementation wraps `signal.Notify` and returns an idempotent cleanup function that calls `signal.Stop` before closing any internally owned channel. A fake source gives tests deterministic control without exposing this mechanism publicly.

A mapping is copied during construction so caller mutation cannot introduce a data race:

```go
type Mapping[E any] map[os.Signal]E

var (
	ErrAlreadyStarted = errors.New("signal adapter already started")
	ErrNoMappings     = errors.New("signal adapter has no mappings")
)
```

The implementation should use a mutex-protected lifecycle state or an atomic compare-and-swap. The transition is terminal:

```text
new -> started -> stopped
  \-> start-failed
```

`Start` reserves the instance before performing work. Consequently, even a failure after reservation consumes the one permitted start. Constructors should perform all validation that can fail before `Start`, minimizing start-time failures.

For every variant, the listening goroutine:

1. owns its signal registration and calls its cleanup function exactly once;
2. selects on the injected signal stream and a cancellation context;
3. never closes caller-owned channels;
4. terminates all child goroutines before reporting completion; and
5. defines explicit slow-consumer behavior rather than spawning unbounded goroutines.

## Variant 1: Single-Consumer Typed Event Stream

### Description

The adapter returns one receive-only event channel and a completion channel. One goroutine maps registered signals and sends events until cancellation.

```go
type Stream[E any] struct {
	// unexported lifecycle, mapping, source, and cancellation fields
}

func NewStream[E any](mapping Mapping[E], buffer int) (*Stream[E], error)

func (s *Stream[E]) Start(ctx context.Context) (
	events <-chan E,
	done <-chan struct{},
	err error,
)

func (s *Stream[E]) Cancel()
```

The adapter owns and closes `events` and `done`. Delivery is FIFO for signals received by its source. A bounded buffer is configured at construction. When full, the listening goroutine blocks until the caller receives or the context is canceled; the event send must select on both destinations so cancellation cannot leak the goroutine.

Unknown signals are ignored defensively, though the production source registers only mapped signals.

### Concurrency and testability

- A lock or atomic state makes concurrent `Start` calls deterministic: one can succeed and all others return `ErrAlreadyStarted`.
- `sync.Once` protects cancellation and signal cleanup.
- A fake source supplies signals; tests consume the returned stream and observe `done`.
- No per-event goroutines are created, making leak and ordering tests straightforward.

### Pros

- Small, idiomatic Go API with clear channel ownership.
- Compile-time event typing and natural `select` composition.
- Preserves ordering and applies bounded backpressure.
- Only one long-lived goroutine is required.

### Cons

- Exactly one logical consumer should receive from the channel.
- A slow consumer can delay subsequent signal handling.
- A caller that starts but never consumes must cancel to allow shutdown.

### Risks

- An incorrectly implemented blocking send can prevent cancellation.
- Buffer sizing may be mistaken for durable queuing; OS signal coalescing already prevents such a guarantee.
- Multiple goroutines reading the event channel divide events rather than broadcast them.

### Usage limitations

- Not suitable for broadcasting every event to multiple independent listeners.
- Not suitable where the signal path must never wait for application processing.
- Does not guarantee one event per signal occurrence because OS and Go signal delivery can coalesce signals.

## Variant 2: Synchronous Callback Adapter

### Description

The caller supplies a handler, and the adapter invokes it serially from its listening goroutine.

```go
type Handler[E any] func(context.Context, E) error

type CallbackAdapter[E any] struct {
	// unexported fields
}

func NewCallbackAdapter[E any](
	mapping Mapping[E],
	handler Handler[E],
) (*CallbackAdapter[E], error)

func (a *CallbackAdapter[E]) Start(ctx context.Context) error
func (a *CallbackAdapter[E]) Cancel()
func (a *CallbackAdapter[E]) Done() <-chan struct{}
func (a *CallbackAdapter[E]) Err() error
```

`Start` launches listening and returns after successful registration. Handlers run one at a time and preserve receive order. The first handler error stops the adapter and becomes `Err`; ordinary cancellation yields a nil terminal error. The handler receives a context canceled when the adapter stops, but cancellation cannot forcibly terminate a handler that ignores its context.

### Concurrency and testability

- Permanent start reservation and idempotent cancellation follow the common foundation.
- Serial invocation avoids concurrent calls into caller code.
- Tests inject both the signal source and a recording handler.
- Completion is observable through `Done`, and terminal failure through `Err` after `Done` closes.

### Pros

- Simplest consumption model for applications that already have a lifecycle handler.
- No event channel or forwarding loop in caller code.
- Explicit handler errors provide a natural fail-fast policy.
- Serial execution avoids handler-level ordering races.

### Cons

- Handler latency directly delays signal processing.
- Callback code runs in an adapter-owned goroutine.
- Correct cancellation depends on a cooperative handler.
- Less composable than a receive-only channel.

### Risks

- A blocked or deadlocked handler leaks the listening goroutine despite adapter cancellation.
- Handler panic can terminate the process unless recovered; recovery policy can obscure programmer defects.
- Calling adapter methods from the handler can introduce reentrancy mistakes if implementation locks are held during invocation.

### Usage limitations

- The constructor contract must require the handler to return promptly and honor context cancellation.
- The adapter should not recover handler panics by default; panic recovery belongs at the application boundary.
- Unsuitable for parallel or broadcast processing without caller-managed dispatch.

## Variant 3: Bounded Pub/Sub Broker

### Description

The adapter broadcasts each mapped event to independently subscribed listeners. Subscribers have bounded queues and an explicit overflow policy.

```go
type OverflowPolicy uint8

const (
	DropNewest OverflowPolicy = iota
	CancelSlowSubscriber
)

type Subscription[E any] interface {
	Events() <-chan E
	Cancel()
}

type Broker[E any] struct {
	// unexported fields
}

func NewBroker[E any](
	mapping Mapping[E],
	queueSize int,
	overflow OverflowPolicy,
) (*Broker[E], error)

func (b *Broker[E]) Subscribe() (Subscription[E], error)
func (b *Broker[E]) Start(ctx context.Context) error
func (b *Broker[E]) Cancel()
func (b *Broker[E]) Done() <-chan struct{}
```

Subscriptions are allowed before or after `Start` and rejected after shutdown begins. A single broker loop owns the subscriber registry, avoiding concurrent map access. Subscribe, unsubscribe, signal, and cancellation requests pass through bounded control channels. Each subscriber has a bounded event channel, but no dedicated goroutine.

Broadcast delivery is non-blocking. `DropNewest` skips an event for a full subscriber. `CancelSlowSubscriber` removes that subscriber and closes its event channel. Events retain order within each subscription, but subscribers can observe different sets under pressure.

### Concurrency and testability

- The broker loop is the sole mutator of subscription state and closes all subscriber channels during shutdown.
- Subscription cancellation is idempotent and waits only for the broker to accept the request or stop.
- Tests inject signals, create controlled slow/fast subscribers, and verify overflow and shutdown behavior.
- Race tests stress concurrent subscription, unsubscription, start, and cancellation.

### Pros

- Native one-to-many delivery with independent consumers.
- A slow listener cannot block signal intake or other listeners.
- Bounded memory and no goroutine per subscriber.
- Subscriber lifecycle is explicit.

### Cons

- Larger API and substantially more state-machine complexity.
- Delivery may be lossy under load.
- The broker loop can become a throughput bottleneck with many subscribers.
- Subscription setup and teardown require careful shutdown coordination.

### Risks

- Incorrect channel-closing ownership can cause panics.
- Control-channel saturation can make subscribe or cancel operations wait.
- Consumers may overlook dropped events unless observability is added.
- Fairness across a very large subscriber set is not guaranteed.

### Usage limitations

- Not appropriate when every event must reach every subscriber.
- Subscription queues and overflow policy are broker-wide in this design.
- Events are delivered only after a subscription is accepted; there is no replay.
- Subscriber counts should remain within a documented operational bound established by benchmarks.

## Variant 4: Dispatcher with Pluggable Sink

### Description

Signal acquisition and event delivery are separated by a narrow sink contract. This supports channels, callbacks, queues, or application-specific fan-out without embedding all policies in the signal adapter.

```go
type Sink[E any] interface {
	Deliver(context.Context, E) error
	Close() error
}

type Dispatcher[E any] struct {
	// unexported fields
}

func NewDispatcher[E any](
	mapping Mapping[E],
	sink Sink[E],
) (*Dispatcher[E], error)

func (d *Dispatcher[E]) Start(ctx context.Context) error
func (d *Dispatcher[E]) Cancel()
func (d *Dispatcher[E]) Done() <-chan struct{}
func (d *Dispatcher[E]) Err() error
```

The dispatcher calls `Deliver` serially. A delivery error stops listening and becomes the terminal error. On exit, it calls `Close` once; a prior delivery error takes precedence over a close error. Implementations may provide standard bounded channel and callback sinks, while sophisticated users can supply a broker-backed sink.

The sink contract requires `Deliver` and `Close` not to call each other, to honor cancellation, and to be safe for the documented serial invocation. The dispatcher never invokes both concurrently.

### Concurrency and testability

- Adapter lifecycle remains independent of delivery implementation.
- Fake sources and recording/failing sinks allow deterministic tests of both sides.
- Sink contract tests can be shared by standard sink implementations.
- Cancellation is included in every `Deliver` context, allowing bounded blocking without detached goroutines.

### Pros

- Clean separation between signal mechanics and delivery policy.
- Extensible without expanding the core adapter API.
- Enables focused unit and contract testing.
- Applications can integrate existing queue or event infrastructure.

### Cons

- More abstraction and configuration than most callers need.
- Safety depends partly on third-party sink behavior.
- Error and shutdown precedence must be carefully documented.
- A custom sink can hide unbounded queues or leaked goroutines.

### Risks

- A sink that ignores context can block shutdown indefinitely.
- `Close` implementations may deadlock or panic if their ownership contract is unclear.
- Overgeneralization can make simple signal handling harder to understand.

### Usage limitations

- The adapter cannot guarantee leak freedom for a non-conforming custom sink.
- Delivery is serial unless the sink internally adds concurrency.
- Custom sinks must document durability, ordering, overflow, and channel ownership themselves.

## Comparative Analysis

| Variant | Delivery model | Backpressure / overflow | Fan-out | Complexity | Best fit |
|---|---|---|---|---|---|
| Typed event stream | Receive-only channel | Bounded, blocking, cancelable | No | Low | Default application integration |
| Callback adapter | Serial handler | Handler latency blocks intake | No | Lowest for caller | One simple lifecycle handler |
| Pub/sub broker | Per-subscriber channels | Drop or cancel slow subscriber | Yes | High | Many concurrent independent listeners |
| Pluggable dispatcher | Caller-selected sink | Defined by sink | Sink-dependent | Medium/high | Integration with established event infrastructure |

## Recommendation

Use **Variant 1, the single-consumer typed event stream**, by default. It is the smallest generally useful abstraction, composes naturally with `context` and `select`, keeps ownership visible, preserves ordering, and can guarantee bounded memory and cancellation without delegating safety to caller code.

Do not use it when every listener must independently observe events or when a slow consumer must never delay signal intake. Use **Variant 2** for the simplest case: one short, cooperative lifecycle callback and no need to compose the event stream with other channels. Use **Variant 3** for highly concurrent pub/sub workloads where bounded, explicitly lossy delivery is acceptable. Use **Variant 4** when the application already has a queue, broker, or delivery abstraction and the added interface boundary is justified.

None of these variants should claim durable or exact signal delivery. If missing an event is unacceptable, the signal should initiate reconciliation against durable application state rather than serve as the state change itself.

## Verification and Test Scenarios

Every implementation should include:

- constructor tests for empty mappings, invalid buffer sizes, nil handlers or sinks, and defensive mapping copies;
- concurrent `Start` tests proving exactly one success and `ErrAlreadyStarted` for all other calls;
- tests proving an instance cannot restart after cancellation, normal completion, or start failure;
- cancellation before, during, and after delivery, including repeated concurrent cancellation;
- fake-source tests for mapped signals, unknown signals, ordering, and registration cleanup;
- slow-consumer tests proving the documented backpressure or overflow behavior;
- shutdown tests proving all owned channels close exactly once and all goroutines terminate;
- `go test -race` stress tests for lifecycle and subscriber operations;
- leak checks using bounded timeouts and goroutine-aware test tooling;
- isolated Unix integration tests validating real `os/signal` registration and cleanup without running in parallel with other signal tests.

## Execution Metadata

- Execution mode: planning
- Planning and document production: performed in a single interactive run; precise phase timing was not available.
- Token usage: authoritative input, output, reasoning, and cache token counts were not available to the model.
