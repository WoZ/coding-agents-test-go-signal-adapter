# Architecture proposals: OS signal adapter with event remapping

## Scope and assumptions

This document proposes four package architectures for Go 1.26 on Unix. It is based only on the task statement; no
repository implementation or existing proposal was inspected.

The proposals share these assumptions:

- The caller supplies a finite, non-empty mapping from `os.Signal` to an arbitrary user-defined event type `E`.
- Signal identifiers may appear in the public configuration because they are domain inputs. The package does not expose
  `signal.Notify`, its registration channel, or its cleanup rules.
- Event values are treated as immutable after construction. The adapter copies the mapping, but a shallow copy cannot
  isolate pointers, slices, maps, or other reference-bearing values inside `E`.
- An OS signal notification is not a durable message. Unix and Go may coalesce or drop repeated signals. No variant
  claims exactly-once delivery or one event per signal occurrence.
- Shutdown is cooperative. `Cancel` is safe and idempotent, and completion can be observed through a closed output
  channel or `Wait`. A bounded amount of already-selected work may finish during a cancellation race.
- Calling `Cancel` before a successful start is a no-op. A successful first start permanently consumes the object's
  start capability. A later start returns `ErrAlreadyStarted`, including after shutdown.
- Configuration and registration errors discovered before a successful start do not consume the start capability.
  Production registration has no expected recoverable error, but this rule makes injected test failures well-defined.
- The production implementation supports the signals provided by the target Unix platform. Tests must not assume that
  every Unix defines the same `syscall.SIG*` constants.

## Common design foundation

### Public event mapping

A generic mapping keeps event ownership with the caller:

```go
package signaladapter

import "os"

type Mapping[E any] map[os.Signal]E

var (
	ErrAlreadyStarted = errors.New("signal adapter has already been started")
	ErrNotStarted     = errors.New("signal adapter has not been started")
	ErrStopped        = errors.New("signal adapter has stopped")
)
```

`New` validates that the map is non-empty and contains no nil signal, then clones it. Options are validated and copied
too. Constructors return errors rather than panic for invalid caller input. The package must document that mapped event
values must not be mutated concurrently.

Using `E any` is intentional: events need not be comparable. Returning the mapped value preserves its static type and
avoids forcing callers through `any` and type assertions.

### Lifecycle

Every variant uses the same monotonic state machine:

```text
new --successful start--> running --cancel/context/source exit--> stopped
 |                         |                                      |
 +--setup failure--> new   +--Start: ErrAlreadyStarted------------+
 +--Cancel: no-op          +--Cancel: initiate shutdown           |
                                                                Start:
                                                        ErrAlreadyStarted
```

State changes are serialized by a small mutex, or by an atomic compare-and-swap plus a mutex for the associated
resources. A plain boolean is insufficient because `Start`, `Cancel`, and status observation may race. `sync.Once` alone
is also a poor fit: it cannot naturally distinguish setup failure from a successful, permanently consumed start.

The transition to `running` occurs only after the internal source registration succeeds. No path transitions from
`stopped` to `new`. `Cancel` copies the current cancel function while holding the lock, releases the lock, and then
invokes it; package code never calls user code or performs a potentially blocking operation while holding the lifecycle
mutex.

### Hidden signal source and deterministic tests

The package hides `os/signal` behind an unexported port:

```go
type signalSource interface {
    Register(signals []os.Signal, capacity int) (
        events <-chan os.Signal,
        stop func (),
        err error,
    )
}
```

The production source creates and owns the writable channel, calls `signal.Notify`, and returns an idempotent `stop`
closure that calls `signal.Stop` exactly once. Adapter code must invoke `stop` on every terminal path. Neither this
interface nor a source option needs to be exported; package tests use `newWithSource`, while external users see only the
adapter abstraction.

A fake source can emit signals deterministically, report registration and stop calls, fail setup, and close
unexpectedly. This enables tests without sending process-wide signals. A small integration test may use a subprocess to
exercise real Unix delivery without interfering with the test runner.

For every variant, tests should cover:

- mapping validation and defensive copying;
- correct remapping and ignoring signals that were not registered;
- simultaneous `Start` calls, proving exactly one success;
- a second start while running and after stopping;
- `Start` racing with `Cancel`;
- context cancellation and repeated concurrent `Cancel` calls;
- source cleanup exactly once on every exit;
- output closure or `Wait` completion;
- blocked consumer or full-buffer shutdown;
- execution under `go test -race`;
- repeated create/start/cancel cycles with bounded goroutine-count or leak checks.

Tests should use deadlines only as failure bounds, not sleeps as synchronization.

## Variant 1: single-consumer event channel

### Description

This is the conventional Go API and the recommended default. `Start` registers the mapped signals, starts one pump
goroutine, and returns a receive-only event channel:

```go
type Adapter[E any] struct {
    // Unexported immutable configuration and synchronized lifecycle state.
}

func New[E any](mapping Mapping[E], opts ...Option) (*Adapter[E], error)

func (a *Adapter[E]) Start(ctx context.Context) (<-chan E, error)
func (a *Adapter[E]) Cancel()
```

The output buffer size is an option with a documented, finite default. The internal signal channel is also bounded. The
pump owns and closes the output channel. Its loop conceptually performs:

```go
defer stopSource()
defer close(out)
defer markStopped()

for {
    select {
    case <-runCtx.Done():
        return
    case sig, ok := <-signals:
        if !ok {
            return
        }
        event := mapping[sig]
        select {
            case out <- event:
            case <-runCtx.Done():
                return
        }
    }
}
```

Only mapped signals are registered. The map lookup should still check `ok` defensively so a faulty injected source
cannot create the zero value of `E` as a false event.

`Cancel` cancels the private run context. Deriving a private context rather than storing the caller's cancel function
gives the adapter an independent, idempotent shutdown control. The send selects on cancellation, so a caller that
abandons a full output channel cannot strand the pump. Source cleanup and output closure occur in the pump's defers.

The channel is intentionally single-stream. Multiple goroutines may receive from it safely, but they compete for events;
it does not broadcast a copy to each receiver.

### Pros

- Idiomatic, small API that composes directly with `select`.
- Exactly one package-owned goroutine per running adapter.
- Natural backpressure with a bounded memory footprint.
- Cancellation unblocks both a signal receive and a blocked event send.
- Static event typing and no callback reentrancy.
- Straightforward deterministic testing with the fake source.

### Cons

- A slow or abandoned consumer fills the output buffer and pauses adapter delivery.
- Multiple channel receivers load-balance rather than broadcast.
- A goroutine is required even when signals are rare.
- The channel reports normal completion but not a rich terminal cause unless an additional status API is added.

### Risks

- Users may incorrectly assume the output is a durable queue. Documentation must state the OS-level loss/coalescing
  behavior.
- Choosing an excessively large buffer hides slow consumers and increases retained memory; choosing a tiny buffer
  increases loss pressure upstream.
- Closing the output from `Cancel` instead of from its sole producer would introduce a send/close race. Channel
  ownership must remain with the pump.
- Mutable reference-bearing event values can still race in caller code despite the mapping clone.

### Usage limitations

- Use only when each mapped event should be handled once by one logical consumer.
- Do not use one returned channel as a broadcast mechanism.
- Consumers must keep reading until closure or tolerate events being discarded as the process exits.
- Delivery order is the order observed from the internal signal stream, not a guarantee about exact kernel occurrence
  order.
- Cancellation may race with a ready send, so at most the event already selected by the pump may still be observed.

## Variant 2: synchronous pull cursor

### Description

This variant minimizes package-owned concurrency. `Start` performs registration, and callers explicitly pull one mapped
event at a time:

```go
type Cursor[E any] struct {
    // Unexported source, mapping, lifecycle, and single-reader gate.
}

func NewCursor[E any](mapping Mapping[E], opts ...Option) (*Cursor[E], error)

func (c *Cursor[E]) Start(ctx context.Context) error
func (c *Cursor[E]) Next(ctx context.Context) (E, error)
func (c *Cursor[E]) Cancel()
```

`Next` selects among the source channel, the adapter's lifetime cancellation, and the per-call context. A per-call
timeout therefore cancels only that wait; it does not stop the cursor. Lifetime cancellation returns `ErrStopped`.
Calling `Next` before `Start` returns `ErrNotStarted`.

Only one `Next` may be active. A non-blocking atomic gate returns a documented `ErrConcurrentNext` rather than silently
serializing callers for an unbounded time. This makes ownership mistakes visible and keeps event ordering clear.

The cursor does not need a standing pump goroutine. To ensure that cancellation of the context passed to `Start` still
unregisters the source even when no caller is inside `Next`, it registers a `context.AfterFunc` callback that invokes
`Cancel`. `Cancel` stops that callback when appropriate, calls the idempotent source cleanup, closes an internal `done`
channel once, and marks the cursor stopped. `context.AfterFunc` creates no waiting goroutine for each cursor;
cancellation may briefly run the callback.

If the source closes unexpectedly, `Next` performs the same cleanup and returns a terminal source error. Source cleanup
and the `done` close are protected by `sync.Once`.

### Pros

- No standing adapter goroutine.
- The caller controls where and when blocking occurs.
- Per-call deadlines and cancellation are explicit.
- Minimal buffering and retained memory.
- A good fit for a simple command loop already owned by the caller.
- Easy to test one transition or event at a time.

### Cons

- More procedural than receiving from a channel.
- Exactly one active reader is supported.
- The caller must write and own the receive loop.
- `context.AfterFunc` and cleanup ordering are subtler than the channel pump.
- It does not naturally compose as a case in a larger `select`; `Next` itself owns the select.

### Risks

- A caller that starts the cursor but neither calls `Cancel` nor eventually cancels the start context retains the signal
  registration.
- Incorrectly closing `done` from both the source-close and cancellation paths would panic; cleanup must be unified
  behind `sync.Once`.
- A per-call context could be mistaken for the lifetime context. Names and documentation must distinguish them.
- As with all variants, a signal burst can be coalesced or dropped before `Next` receives it.

### Usage limitations

- `Next` is intentionally not concurrent; use Variant 1 or 3 for multiple consuming goroutines.
- Every successful `Start` must be paired with `Cancel` or a lifetime context that will be canceled.
- There is no broadcast, parallel processing, or subscription management.
- It is best when signal handling belongs in one existing control loop and event volume is low.

## Variant 3: actor-owned pub/sub router

### Description

This variant broadcasts each mapped event to multiple independent subscribers. A single router actor owns the subscriber
registry and all subscriber-channel closure, eliminating send/close races:

```go
type OverflowPolicy uint8

const (
    DropNewest OverflowPolicy = iota
    DropOldest
    DisconnectSlow
    Block
)

type Router[E any] struct {
    // Unexported configuration, lifecycle, and actor command channel.
}

type Subscription[E any] struct {
    // Unexported receive channel and idempotent unsubscribe operation.
}

func NewRouter[E any](mapping Mapping[E], opts ...RouterOption) (*Router[E], error)

func (r *Router[E]) Start(ctx context.Context) error
func (r *Router[E]) Subscribe(buffer int, policy OverflowPolicy) (*Subscription[E], error)
func (r *Router[E]) Cancel()
func (r *Router[E]) Wait() error

func (s *Subscription[E]) Events() <-chan E
func (s *Subscription[E]) Close()
```

To avoid a goroutine created by `New` that could be forgotten, subscriptions are accepted only after successful `Start`.
`Start` launches one actor goroutine. `Subscribe` sends a command to that actor and waits for an acknowledgement while
also selecting on router shutdown. `Subscription.Close` sends an idempotent unsubscribe command. The actor alone
adds/removes subscriptions, sends events, and closes subscriber channels.

The actor selects among source signals, subscription commands, and cancellation. For each mapped event, it walks its
owned subscriber set and applies each subscription's explicit bounded-buffer policy:

- `DropNewest`: retain queued events and discard the new event.
- `DropOldest`: discard one queued event and attempt to enqueue the new event.
- `DisconnectSlow`: remove the subscriber and close its channel.
- `Block`: wait for space or router cancellation.

The actor keeps per-subscription counters for dropped events. These may be exposed as an immutable `SubscriptionStats`
snapshot so overload is observable. A zero-capacity subscription is valid only with `Block`; rejecting ambiguous
combinations during `Subscribe` is safer than surprising behavior.

The router closes all active subscription channels and unregisters the signal source before `Wait` completes. `Wait`
must not hold a mutex while waiting. It returns `ErrNotStarted` before start, then the stable terminal error after
shutdown.

### Pros

- True broadcast semantics: every active subscriber is independently considered.
- One actor owns mutable routing state and channel closure, greatly reducing race complexity.
- Slow-subscriber behavior is explicit, bounded, and configurable.
- Dynamic subscribe/unsubscribe is safe during concurrent delivery.
- Per-subscriber overload metrics make loss visible.
- One core goroutine handles many subscribers; no goroutine per subscription is required.

### Cons

- Significantly larger API and state space than a single event channel.
- Broadcast work is `O(number of subscribers)` per event.
- The `Block` policy lets one subscriber delay all subscribers.
- Loss policies complicate application semantics and testing.
- Subscription creation is unavailable before `Start` under this lifecycle design.

### Risks

- Fairness is not guaranteed when iterating subscribers; a fixed iteration order can systematically favor some
  subscribers. The actor should rotate its starting point or document the behavior.
- `DropOldest` requires the actor to remove from a channel that a subscriber also receives from. This is
  concurrency-safe, but exact queue contents are timing-dependent and should be specified as best effort.
- An unbounded subscriber buffer would violate bounded-memory requirements and must not be offered.
- If command acknowledgement sends are not cancellation-aware, a caller abandoning `Subscribe` could strand the actor.
  Each command response must be buffered or selected against shutdown.
- A very large subscriber set can delay command processing and cancellation checks; the delivery loop should check
  cancellation between subscribers.

### Usage limitations

- Use only when independent listeners must each see an event.
- Subscribers must choose and accept an overload policy; no policy can create durable delivery from OS signals.
- `Block` is appropriate only when every subscriber is trusted to drain promptly.
- The actor preserves observed event order per subscriber unless that subscriber uses a dropping policy.
- This is in-process pub/sub, not cross-process messaging, persistence, replay, or acknowledgement.

## Variant 4: synchronous dispatcher/sink bridge

### Description

This variant integrates the adapter with an existing application dispatcher without exposing a channel. The package owns
no processing goroutine: the caller runs `Run`, which registers signals and dispatches mapped values synchronously until
cancellation or failure.

```go
type Dispatcher[E any] interface {
    Dispatch(context.Context, E) error
}

type Runner[E any] struct {
    // Unexported source, mapping, dispatcher, and lifecycle.
}

func NewRunner[E any](
    mapping Mapping[E],
    dispatcher Dispatcher[E],
    opts ...RunnerOption,
) (*Runner[E], error)

func (r *Runner[E]) Run(ctx context.Context) error
func (r *Runner[E]) Cancel()
```

`Run` is the single-start method. It normally executes in a caller-owned goroutine such as an `errgroup.Group` task. It
selects on source signals and the private run context, remaps each signal, and calls:

```go
if err := dispatcher.Dispatch(runCtx, event); err != nil {
    return fmt.Errorf("dispatch mapped signal event: %w", err)
}
```

`Run` defers source cleanup and the transition to `stopped`. `Cancel` cancels `runCtx`; it does not close a channel or
invoke the dispatcher itself.

The dispatcher contract is important: `Dispatch` must return when its context is canceled and must not retain or mutate
the supplied event unless its own API explicitly permits that. The adapter calls it serially, so observed ordering is
preserved and the dispatcher need not be concurrency-safe for this use.

Applications can adapt an existing bus, state machine, logger-reload controller, or lifecycle coordinator behind
`Dispatcher[E]`. The adapter stays unaware of downstream implementation details.

### Pros

- Direct integration with an existing event bus or application boundary.
- No package-owned goroutine and no exposed event channel.
- Backpressure and dispatch failures are explicit synchronous outcomes.
- Serial calls preserve observed ordering and avoid concurrent callback reentrancy.
- The dispatcher is a narrow, easily faked test seam.

### Cons

- A slow dispatcher pauses receipt of further notifications.
- Correct shutdown depends on the dispatcher honoring context cancellation.
- The caller must allocate a goroutine if `Run` must execute alongside other work.
- One dispatcher is supported; fan-out belongs inside that dispatcher.
- Error policy is necessarily opinionated: stop, retry, or ignore must be configured.

### Risks

- A dispatcher that ignores context and blocks forever prevents prompt `Run` completion. The package cannot safely force
  arbitrary user code to return.
- Automatic retries can duplicate events and prolong shutdown. The default should be no retry; any bounded retry option
  needs an explicit policy and cancellation-aware backoff.
- If the application dispatcher also routes back into the adapter, synchronous reentrancy can deadlock at a higher
  layer.
- Treating a dispatch success as proof of durable processing would overstate the contract; it means only what the
  supplied dispatcher defines.

### Usage limitations

- The dispatcher must honor context cancellation and return in bounded time; otherwise the prompt shutdown guarantee
  cannot be met.
- This is unsuitable for untrusted or potentially blocking callbacks.
- Parallel processing and independent subscribers must be implemented downstream, or Variant 3 should be used.
- `Run` cannot be called again after any successful start, regardless of its terminal error.
- The runner should be placed in a caller-managed `errgroup` when its error must cancel sibling components.

## Comparison

| Variant              | Delivery model                        |          Package-owned goroutines | Backpressure                                | Fan-out         | API complexity | Best fit                                            |
|----------------------|---------------------------------------|----------------------------------:|---------------------------------------------|-----------------|----------------|-----------------------------------------------------|
| 1. Event channel     | One stream; competing receivers       |                   1 while running | Bounded channel; producer waits             | No              | Low            | General-purpose default and `select`-based services |
| 2. Pull cursor       | One event per `Next` call             |                     None standing | Caller pulls; source buffer remains bounded | No              | Low–medium     | Simplest low-volume control loop                    |
| 3. Pub/sub router    | Copy considered for every subscriber  |             1 actor while running | Per-subscriber block/drop/disconnect policy | Yes             | High           | Many concurrent independent listeners               |
| 4. Dispatcher bridge | Serial calls into an application port | None; caller owns `Run` goroutine | Synchronous dispatcher return               | Downstream only | Medium         | Existing event bus or lifecycle coordinator         |

All four can be data-race-safe and bounded-memory designs. Variants 1 and 3 have package-owned goroutines, but their
blocking operations always select on cancellation and their terminal paths unregister the signal source. Variants 2 and
4 avoid standing package goroutines; Variant 4 additionally relies on the documented dispatcher cancellation contract
because no Go package can forcibly terminate arbitrary user code.

## Recommendation

Choose **Variant 1, the single-consumer event channel**, as the default. It is the smallest broadly composable API,
follows familiar Go channel ownership rules, works naturally in `select`, has one clear shutdown path, and supports
bounded backpressure without imposing callback behavior on the caller. Its semantics are also difficult to misuse when
described explicitly: it is one stream, not broadcast.

Do not use Variant 1 when multiple independent components must each receive every event, when package-owned goroutines
are prohibited, or when the application already has a dispatcher boundary that should own routing and errors.

For the simplest cases, choose **Variant 2**. It adds no standing adapter goroutine and makes a low-volume, single-owner
control loop explicit. Its one-active-`Next` restriction is a useful simplification rather than hidden serialization.

For highly concurrent pub/sub cases, choose **Variant 3**. Its actor ownership model gives dynamic subscriptions and
safe channel closure without a mutex/send race, while bounded per-subscriber policies prevent one forgotten listener
from causing unbounded memory growth. Use `DropNewest` or `DisconnectSlow` as the general production policy; reserve
`Block` for small sets of trusted subscribers.

Choose **Variant 4** when mapped signals should enter an established application bus or lifecycle coordinator and that
dispatcher has a strong, cancellation-aware contract. It is an integration architecture, not the general default.

## Implementation invariants

Whichever variant is selected, code review should enforce these invariants:

1. The mapping and option values are validated and defensively copied before publication to concurrent code.
2. Exactly one successful start transition is possible for an object over its lifetime.
3. All lifecycle reads and writes are synchronized; tests passing without `-race` is not evidence of synchronization.
4. The production source registration is stopped exactly once on every terminal path.
5. Only the goroutine or actor that sends on a channel may close it.
6. Every potentially blocking internal send or receive has a shutdown alternative.
7. No buffer, retry queue, or subscriber queue is unbounded.
8. Package locks are not held across channel operations, waits, source calls, or user-supplied code.
9. Cancellation is idempotent, concurrent-safe, and observable through channel closure, `Next`, `Wait`, or `Run`.
10. Documentation explicitly rejects exactly-once and durable-delivery interpretations.

## Execution metadata

- Model/effort identifier supplied for the output: `codex-5.5-xhigh`
- Analysis mode: default mode
- Default-mode drafting and review: approximately 2 minutes 44 seconds (`2026-07-09T13:00:11Z` to
  `2026-07-09T13:02:55Z`)
- Planning-mode elapsed time: not applicable; the task ran in default mode
- Input/output/cache token counts: not exposed by the execution interface
