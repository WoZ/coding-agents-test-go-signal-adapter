# Architecture proposals: OS signal adapter with event remapping

## Scope and assumptions

These proposals target Go 1.26 on Unix. They deliberately hide `os/signal` behind package APIs and accept caller-defined
event values through a generic type parameter.

Common assumptions:

- Signal-to-event mappings are immutable after construction. This removes synchronization from the read path and makes
  ownership clear.
- One OS signal maps to one event. Supporting one-to-many mappings belongs in a dispatcher layer.
- Event values are safe to copy or are treated as immutable by callers.
- Cancellation stops package-owned goroutines and unregisters signal delivery.
- `Start` has lifetime-wide, at-most-once semantics: after the first call, every later call returns `ErrAlreadyStarted`,
  even if the first run has stopped.
- A zero mapping is rejected at construction because a listener with no useful output is likely a configuration error.
- Shutdown races may discard a signal already buffered internally. No proposal promises delivery after cancellation
  begins.
- Unix signal behavior remains subject to process-wide constraints: uncatchable signals such as `SIGKILL` and `SIGSTOP`
  cannot be handled.

The implementation should expose a small internal signal source interface so tests do not send real process signals:

```go
type signalSource interface {
Notify(chan<- os.Signal, ...os.Signal)
Stop(chan<- os.Signal)
}
```

Production code adapts `signal.Notify` and `signal.Stop`; tests inject a fake. The source channel should be buffered,
owned by the listener, passed unchanged to `Stop`, and never closed by the package.

## Common lifecycle model

All variants use an atomic compare-and-swap or a mutex-protected state transition from `new` to `started`. There is no
transition back to `new`.

```go
var ErrAlreadyStarted = errors.New("signal listener already started")

func (l *Listener[T]) Start(ctx context.Context) error {
if !l.started.CompareAndSwap(false, true) {
return ErrAlreadyStarted
}
// Register signals, start owned goroutines, and arrange cleanup.
return nil
}
```

Construction validates the mapping, copies it, and derives the exact signal registration set. `Start` must either
establish the run or perform complete cleanup if startup fails. A `Wait` or blocking `Run` contract must define when all
goroutines have exited. Cleanup calls `signal.Stop`, cancels internal contexts, and waits for package-owned goroutines.

## Variant 1: blocking `Run` with callback

### Description

The simplest API blocks in the caller's goroutine and invokes one handler serially:

```go
type Handler[T any] func (context.Context, T) error

func NewRunner[T any](mapping map[os.Signal]T, handler Handler[T], opts ...Option) (*Runner[T], error)
func (r *Runner[T]) Run(ctx context.Context) error
```

`Run` is the single start operation. It registers signals, receives until cancellation or handler failure, maps each
signal, and invokes the handler. Deferred cleanup unregisters notification before returning. Handler errors stop the run
and are wrapped with context.

### Pros

- Minimal API and no package-owned long-lived goroutine.
- Backpressure is explicit: signal processing pauses while the handler runs.
- Cleanup and completion have one obvious boundary: `Run` returning.
- Straightforward deterministic tests with a fake source and synchronous handler.

### Cons

- A slow handler delays all later events.
- The caller must dedicate a goroutine if non-blocking startup is required.
- A callback is less composable than a receive-only channel.

### Risks

- Blocking forever inside a handler prevents timely shutdown; context cooperation cannot be enforced.
- The OS notification buffer can fill while a handler is slow, and OS signal delivery may coalesce.
- Calling arbitrary user code in the receive loop can amplify panic impact; panic recovery policy must be documented
  rather than silently chosen.

### Usage limitations

- One serial consumer only.
- Unsuitable when independent subscribers or parallel event handling are required.
- Delivery is at-most-once from adapter to handler, with no durability guarantee.

## Variant 2: owned listener with receive-only event channel

### Description

The listener starts one translation goroutine and exposes a bounded receive-only channel:

```go
type Listener[T any] struct { /* unexported */ }

func New[T any](mapping map[os.Signal]T, opts ...Option) (*Listener[T], error)
func (l *Listener[T]) Start(ctx context.Context) (<-chan T, error)
func (l *Listener[T]) Wait() error
```

On successful `Start`, the goroutine receives signals, looks up immutable mappings, and sends events to an owned output
channel. Cancellation is selected both while receiving and while sending, ensuring a blocked consumer cannot leak the
goroutine. Cleanup unregisters signals, closes the event channel exactly once, records the terminal error, and releases
`Wait`.

The default output capacity should be documented and configurable with a validated option. The default overflow behavior
is blocking backpressure; alternative drop policies should be explicit separate options with observable drop accounting.

### Pros

- Idiomatic Go composition through channels and `select`.
- Small surface while supporting asynchronous consumers.
- Clear ownership: the package closes its output; callers only receive.
- Cancellation-aware sends prevent goroutine leaks.

### Cons

- A consumer that stops reading can stall delivery until cancellation.
- Multiple receivers load-balance rather than broadcast.
- Buffer sizing and overflow policy affect observable behavior.

### Risks

- Returning the channel before signal registration completes creates a startup ambiguity; registration must complete
  before `Start` returns.
- Incorrect cleanup ordering can allow sends after close or retain signal registration.
- Users may assume every OS signal is delivered, although Unix/Go signal notification can coalesce signals.

### Usage limitations

- Intended for one logical consumer, or deliberate competing consumers.
- No replay, persistence, acknowledgement, or broadcast.
- Callers must continue receiving or cancel the context.

## Variant 3: synchronous adapter plus caller-owned sink

### Description

This variant separates signal translation from delivery by requiring a caller-provided sink:

```go
type Sink[T any] interface {
    Publish(context.Context, T) error
}

func NewAdapter[T any](mapping map[os.Signal]T, sink Sink[T], opts ...Option) (*Adapter[T], error)
func (a *Adapter[T]) Start(ctx context.Context) error
func (a *Adapter[T]) Wait() error
```

The adapter owns one receive goroutine but no event queue. It translates each signal and calls `Publish` serially. The
sink determines buffering, fan-out, retry, or persistence. A sink error terminates the adapter unless an explicit,
bounded error policy says otherwise.

### Pros

- Clean separation between OS integration and delivery semantics.
- Integrates with an existing event bus without exposing `os/signal`.
- Easy to unit-test using fake source and recording sink.
- Avoids imposing a second queue when the application already has one.

### Cons

- More abstraction and configuration than simple applications need.
- Correct shutdown depends partly on the sink honoring context cancellation.
- Delivery guarantees cannot be described without also describing the sink.

### Risks

- A sink that ignores context can leak the adapter goroutine during shutdown.
- Retries inside the adapter can duplicate events and indefinitely block later signals.
- Interface-based designs can become overly general; keep the sink contract to one method.

### Usage limitations

- Publication is serial unless concurrency is implemented by the sink.
- The adapter cannot guarantee subscriber delivery, only successful publication.
- Best when the application already owns an event-distribution abstraction.

## Variant 4: bounded pub/sub broker

### Description

The package owns a signal receiver and a broker. Subscribers obtain independent bounded channels:

```go
type Subscription[T any] interface {
    Events() <-chan T
    Close()
}

func NewBroker[T any](mapping map[os.Signal]T, opts ...Option) (*Broker[T], error)
func (b *Broker[T]) Start(ctx context.Context) error
func (b *Broker[T]) Subscribe(opts ...SubscribeOption) (Subscription[T], error)
func (b *Broker[T]) Wait() error
```

One broker goroutine owns the subscriber registry, so subscription changes and fan-out do not race.
Subscribe/unsubscribe commands travel through internal channels with cancellation-aware replies. Each subscription has a
bounded buffer. A documented policy handles a full subscriber buffer: drop newest, drop oldest, or disconnect slow
subscriber. Blocking the entire broker should not be the default because one subscriber would stall all others.

Cancellation stops new subscriptions, unregisters signals, closes all subscription channels, and waits for every owned
goroutine. Prefer a single broker goroutine; per-subscriber forwarding goroutines increase leak and lifecycle
complexity.

### Pros

- True broadcast with independent subscriber buffers.
- Registry ownership provides a race-free concurrency model.
- Explicit slow-subscriber policies isolate consumers.
- Suitable for many concurrent listeners.

### Cons

- Largest API and implementation surface.
- Buffer policy makes loss behavior unavoidable and more visible.
- Subscription lifecycle adds operational and testing complexity.

### Risks

- Unclosed subscriptions retain buffers until broker shutdown.
- Command-channel designs can deadlock if requests do not select on broker cancellation.
- Fairness across heavy subscription churn and signal fan-out requires targeted tests.
- Per-subscriber delivery order is maintainable, but global timing across subscribers is not.

### Usage limitations

- In-memory, non-durable, best-effort pub/sub only.
- Slow subscribers may lose events or be disconnected according to policy.
- Not appropriate when delivery acknowledgement, replay, or cross-process distribution is required.

## Comparison

| Variant              | API complexity | Delivery shape       | Backpressure                             | Best fit                        |
|----------------------|---------------:|----------------------|------------------------------------------|---------------------------------|
| 1. Blocking callback |            Low | One serial handler   | Handler blocks receiver                  | Small command/service lifecycle |
| 2. Event channel     |     Low–medium | One logical consumer | Bounded channel blocks or explicit drops | General default                 |
| 3. Caller-owned sink |         Medium | Defined by sink      | Sink-defined                             | Existing application event bus  |
| 4. Pub/sub broker    |           High | Broadcast            | Per-subscriber bounded policy            | Highly concurrent local pub/sub |

## Recommended default

Variant 2, the owned listener with a receive-only event channel, is the best default. It uses familiar Go primitives,
hides OS notification mechanics, provides an unambiguous ownership model, and supports cancellation-aware shutdown
without forcing applications into callbacks or a broker abstraction.

Do not use Variant 2 when several consumers must each receive every event, when the application already has a mature
event bus, or when durable delivery is required. Variant 1 is best for simpler cases with one short, serial handler.
Variant 4 is best for highly concurrent in-process pub/sub, provided event loss and slow-subscriber behavior are
acceptable and explicit. Variant 3 is preferable when distribution already belongs to another component.

## Test strategy

Every variant should be tested with an injected fake signal source; real OS signal tests should be a small, isolated
integration suite.

Required tests include:

- Concurrent `Start` calls: exactly one succeeds and all others return `ErrAlreadyStarted`.
- A second `Start` after normal completion or cancellation still fails.
- Cancellation before and after registration completes.
- Cancellation while blocked on output, handler, sink, or broker commands.
- Mapping correctness for every configured signal and no output for unregistered signals.
- Cleanup invokes source `Stop` exactly once and all output channels close as documented.
- Slow or absent consumers under each configured overflow policy.
- Concurrent subscribe/unsubscribe/fan-out for Variant 4.
- Repeated lifecycle tests under `go test -race`, plus goroutine-leak checks using bounded waits and eventual
  assertions.

Tests should avoid sleeps as synchronization. Fakes should expose registration and stop barriers, and assertions should
use bounded contexts so failures terminate.

## Implementation guidance

- Keep exported types and options minimal; reject nil handlers, sinks, mappings, and invalid capacities during
  construction.
- Copy caller-owned maps and slices before retaining them.
- Return sentinel lifecycle errors that support `errors.Is`; wrap operational errors with useful context.
- Do not close the internal `os.Signal` channel. Call `signal.Stop` and let it become unreachable after the receiver
  exits.
- Document ordering, buffering, overflow, cancellation, panic, and terminal-error semantics.
- Avoid holding locks while sending on channels or invoking caller code.
- Use `context.Context` as the first parameter where a method performs a scoped operation; do not store a caller context
  except for the active lifetime established by `Start`.
- Keep cleanup idempotent internally with `sync.Once`, while retaining strict public at-most-once start behavior.

## Execution metadata

- Model/effort identifier supplied for the output filename: `codex-5.5-low`
- Planning/default phase timing: unavailable from the execution environment
- Token usage by input, output, and cache: unavailable from the execution environment
