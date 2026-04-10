# NNQ Design

Pure Ruby NNG built on [protocol-sp](https://github.com/paddor/protocol-sp)
(SP wire protocol) and the Async ecosystem.

## Why

NNG (nanomsg-next-generation) is built on the same distributed-computing
lessons as ZeroMQ but with a simpler, cleaner design: one frame per message,
no security mechanism layer, no multipart. The "fallacies of distributed
computing" (Deutsch/Gosling, 1994) still drive every design decision:

| Fallacy | NNG / NNQ response |
|---|---|
| The network is reliable | Auto-reconnect with exponential backoff; linger drain on close |
| Latency is zero | Async send queues decouple producers from consumers |
| Bandwidth is infinite | High-water marks (HWM) bound queue depth per socket |
| Topology doesn't change | Bind/connect separation; peers come and go freely |
| There is one administrator | No broker required; any topology works peer-to-peer |
| Transport cost is zero | Batched writes reduce syscalls; inproc skips the kernel |
| The network is homogeneous | SP is a wire protocol; interop with libnng, mangos, etc. |

NNQ brings all of this to Ruby without C extensions or FFI.

## Layers

```
+----------------------+
|    Application       |  NNQ::PUSH, NNQ::SUB, etc.
+----------------------+
|    Socket            |  send / receive / bind / connect
+----------------------+
|    Engine            |  connection lifecycle, reconnect, linger
+----------------------+
|    Routing           |  PUSH round-robin, PUB fan-out, REQ/REP, ...
+----------------------+
|    Connection        |  SP handshake, framing
+----------------------+
|    Transport         |  TCP, IPC (Unix), inproc (in-process)
+----------------------+
|  io-stream + Async   |  buffered IO, Fiber::Scheduler
+----------------------+
```

## Task tree

A bare `NNQ::PUSH.new` spawns nothing. The first `#bind` or `#connect`
captures the caller's Async task and lazily creates a
**socket-level `Async::Barrier`** under it. From that point on, every
pump, listener, reconnect loop, and per-connection supervisor lives
under that one barrier, so teardown is a single call — `barrier.stop`
cascade-cancels every descendant at once. The parent capture is
one-shot: subsequent bind/connect calls reuse the same barrier.

All spawned tasks are **transient** so they don't prevent the reactor
from exiting when user code finishes. The reactor stays alive during
linger because `Socket#close` blocks (inside the user's fiber) until
send queues drain.

```
parent_task                            Async::Task.current or Reactor root
|
+-- SocketLifecycle#barrier            Async::Barrier -- single cascade handle
    |
    |-- listener accept loops          one per bind endpoint
    |-- reconnect loops                one per dialed endpoint
    |-- monitor task                   Socket#monitor callback fiber
    |
    |-- conn <ep> supervisor           waits on per-conn barrier, runs lost!
    |
    +-- (ConnectionLifecycle#barrier, nested under socket barrier)
        |-- send pump                  per-connection, work-stealing on socket queue
        +-- recv loop                  reads from peer, enqueues into routing
```

**Two barriers.** `SocketLifecycle#barrier` is the socket-level cascade
handle — `Engine#close` calls `.stop` on it once and every descendant
unwinds. `ConnectionLifecycle#barrier` is a nested per-connection barrier
(its parent is the socket barrier) so a supervisor can detect "one of
*this* connection's pumps exited" via `@barrier.wait { ... }` and
cascade-cancel only that connection's siblings — without taking down
peers on other connections.

**Supervisor pattern.** Each connection has a supervisor task spawned
on the *socket* barrier (deliberately not on the per-conn barrier) that
waits for the first pump to finish and runs `lost!` in `ensure`. Placing
the supervisor outside the per-conn barrier avoids the self-stop footgun:
`tear_down!` can safely call `@barrier.stop` on the per-conn barrier
because the current task (the supervisor) is not a member of it. If the
supervisor were inside, stopping the barrier would raise `Async::Cancel`
on itself synchronously and unwind mid-teardown.

**Send pumps are per-connection, queue is per-socket.** Each connection
runs its own send pump fiber that dequeues from the *socket-level* send
queue and writes to its peer. With N live peers, N pumps race to drain
the one queue — work-stealing. A slow peer's pump just stops pulling
(blocked on its own TCP flush); fast peers' pumps keep draining. This
naturally biases load toward whichever consumers are keeping up, which
is exactly what PUSH should do.

## Engine lifecycle

```
bind/connect
  |
  v
[accepting / reconnecting]  <---+
  |                             |
  v                             |
connection_made                 |
  |-- handshake (SP greeting)   |
  |-- register with routing     |
  +-- start recv loop           |
  |                             |
  v                             |
[running]                       |
  |                             |
  v                             |
connection_lost ----------------+  (auto-reconnect if enabled)
  |
  v
close
  |-- stop listeners
  |-- linger: drain send queues
  |-- close remaining connections
  |-- socket barrier.stop  -- cascade-cancels every pump, supervisor,
  |                           reconnect loop, accept loop in one call
  +-- finish closing
```

**Convergent teardown.** `Socket#close` and peer-disconnect
(supervisor-driven `lost!`) all funnel into the same
`ConnectionLifecycle#tear_down!` with identical ordering: routing
removal → connection close → monitor event → promise resolve →
per-conn `barrier.stop`. The state guard (`:closed`) makes it idempotent
so racing pumps can't double-fire side effects.

**Linger.** On close, send queues are drained for up to `linger` seconds.
`linger=0` (or `nil`) closes immediately.

**Reconnect.** Failed or lost connections are retried with configurable
interval (default 100ms). Supports exponential backoff via a Range
(e.g., `0.1..5.0`). Wall-clock quantized so multiple clients reconnecting
with the same interval wake at the same instant (aligned retries).
Suppressed once `@state` moves to `:closing`.

**Timeouts.** TCP connect uses kernel-level `connect_timeout:` via
`Socket.tcp` — capped at the reconnect interval (floor 0.5s). The SP
handshake is similarly wrapped in `with_timeout(handshake_timeout)` so a
non-NNG service that accepts the TCP connection but never sends a
greeting doesn't block the reconnect loop. On timeout, `tear_down!`
fires with `reconnect: true` so the retry loop picks up.

## Per-socket HWM (not per-connection)

The send queue is **one per socket**, not one per connection. `send_hwm`
bounds that single queue. This is a deliberate design choice shared with
the OMQ sibling project.

The simpler model — one shared queue, N work-stealing pumps — gives:

- **Better PUSH semantics.** Strict per-pipe round-robin is a known
  footgun ("one slow worker stalls the pipeline"). Work-stealing routes
  messages to whichever consumer is ready, which is what a load balancer
  should do.
- **Honest HWM accounting.** `send_hwm = 1000` means 1000 messages, not
  1000 per peer.
- **No staging.** Messages enqueued before any peer connects sit in the
  one queue; the first pump that spawns drains them. No race, no double
  drain, no `prepend`.
- **Same parallelism.** Parallelism comes from N pumps, not N queues.

The single concession: if a pump dequeues a message and its peer dies
before the write completes, the in-flight batch is dropped. This is the
only honest answer given the lack of an end-to-end ack at the SP layer.
Apps that need delivery guarantees layer them on top (REQ/REP, etc.).

## Send pump batching

The send pump reduces syscalls by batching:

```
1. Blocking dequeue (wait for first message)
2. Non-blocking drain of all remaining queued messages (capped)
3. Write batch to connection (buffered, no flush)
4. Flush once
5. Yield to scheduler (fairness across pumps)
```

Under light load, batch size is 1 — no overhead. Under burst load
(producer faster than consumer), the batch grows and flushes are
amortized. Batch caps (256 messages / 256 KB) enforce fairness so a
pre-filled queue doesn't let one pump starve peers.

io-stream auto-flushes its write buffer at 64 KB, so large batches hit
the wire naturally during the write loop. The explicit flush at the end
only pushes the remainder that didn't fill a buffer.

The explicit `Async::Task.current.yield` after each batch ensures peer
pumps get a turn even when the send queue stays non-empty. The yield is
effectively free when the scheduler has no other work.

For fan-out (PUB), each subscriber gets its own bounded queue and pump.
When a subscriber's queue is full, messages are silently dropped for
that peer — matching nng's non-blocking fan-out semantics. A slow
subscriber cannot block fast ones.

## Cancellation safety

The `barrier.stop` cascade can deliver `Async::Cancel` to a send-pump
fiber at any await point. The SP frame format issues two separate
`@io.write` calls per message (header, then body), so a cancel arriving
between them would leave the peer's framer reading a body that never
arrives — unrecoverable without closing the connection.

`Protocol::SP::Connection` wraps every wire-write entry point
(`send_message`, `write_message`, `write_messages`, `write_wire`) in
`Async::Task#defer_cancel`. Cancellation requested during a write is
held until the block exits at a frame boundary, then re-raised normally.
The mechanism is orthogonal to the connection's internal `Mutex` — the
mutex serializes thread races, `defer_cancel` serializes fiber
cancellations. Both are required.

`defer_cancel` only delays cancellation arriving from outside the block.
Exceptions raised by the write itself (`EPIPE`, `EOFError`,
`ECONNRESET`) propagate immediately — that's the peer-disconnect path
the supervisor relies on.

## Transports

**TCP** — standard network sockets. Bind auto-selects port with `:0`.
Connect uses `Socket.tcp` with Happy Eyeballs and kernel-level connect
timeout.

**IPC** — Unix domain sockets. Supports file-based paths. File sockets
are cleaned up on unbind. Uses a 1-byte message-type prefix (nng's
SP/IPC wire format) for compatibility with nng peers.

**inproc** — in-process. Both peers share a Unix socketpair — no network,
no address. Still runs through Protocol::SP (the socketpair replaces TCP),
keeping the transport implementation minimal. Kernel buffering across the
pair handles contention for typical message sizes.

All TCP and IPC connections are wrapped in `IO::Stream::Buffered` which
provides `read_exactly(n)` for reading SP frames and buffered writes for
batch flushing.

## Socket types

| Pattern | Send | Receive | Routing |
|---|---|---|---|
| PUSH/PULL | round-robin (work-stealing) | fair-queue | load balancing |
| PUB/SUB | fan-out (local prefix filter) | subscribe filter | publish/subscribe |
| REQ/REP | round-robin + request ID | request-ID-based reply | request/reply |
| PAIR | exclusive 1:1 | exclusive 1:1 | bidirectional |

All socket types use nng's version-0 protocols (`push0`, `pull0`, etc.)
and are wire-compatible with libnng peers.

## Dependencies

- **async** — Fiber::Scheduler reactor, tasks, promises, barriers, queues
- **io-stream** — buffered IO wrapper (read_exactly, flush, connection errors)
- **protocol-sp** — SP wire protocol codec (greeting, framing, mutex-protected writes)
