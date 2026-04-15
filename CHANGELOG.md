# Changelog

## Unreleased

- **Send-path freezes the body** — every public send method (PUSH,
  PUB, PAIR, BUS, REQ, REP, SURVEYOR, RESPONDENT) routes the body
  through `Socket#frozen_binary`, which coerces to a frozen binary
  string. Fast path: already frozen and binary → returned as-is, no
  allocation. Slow path: `body.b.freeze` (one copy). Prevents a
  caller from mutating the string after it has been enqueued (the
  body can sit in a send queue or per-peer queue until a pump
  writes it).
- **Hot-path: no kwargs splat on verbose monitor emit** —
  `emit_verbose_monitor_event(type, **detail)` replaced with dedicated
  `emit_verbose_msg_sent(body)` / `emit_verbose_msg_received(body)`
  helpers. Early-returns before allocating the detail hash, so the
  send/recv loops pay nothing when `-vvv` is off. Send pump also
  hoists the `verbose_monitor` check out of the batch `.each`.
- **YJIT-friendly `all?` blocks** — `@queues.each_value.all?(&:empty?)`
  → explicit `{ |q| q.empty? }` in pub/bus/surveyor `drained?`
  (YJIT specializes explicit blocks, not `Symbol#to_proc`).
- **`Reactor.run` uses `Async::Promise`** — replaces the
  `Thread::Queue` + manual `[:ok,val]`/`[:error,exc]` tagging with a
  single `result.fulfill { block.call }` + `result.wait` pair.
- **`Engine#spawn_task(parent:)`** — renamed from `barrier:` to make it
  clear any parent barrier is accepted, not just the socket-level one.
- **`linger` default → `Float::INFINITY`** — matches libzmq parity.
  `Socket#close` waits forever for the send queue to drain. Pass
  `linger: 0` for the old drop-on-close behavior.
- **`Socket.new` accepts a block** — File.open-style. The socket is
  yielded to the block and `#close`d when the block returns (or
  raises).
- **`drain_send_queue` rescues `Async::Stop`** — parent-task
  cancellation during close no longer propagates out of the ensure
  path; the rest of teardown runs.
- **Hot-path `Array#first`** — `send_pump` uses `Array#first` instead
  of `[0]` for YJIT specialization.
- **Barrier-based cascading teardown** — `SocketLifecycle` owns a
  socket-level `Async::Barrier`; `ConnectionLifecycle` creates a nested
  per-connection barrier. All pumps, accept loops, reconnect loops, and
  supervisors live under these barriers. `Engine#close` calls
  `barrier.stop` once and every descendant unwinds atomically. Replaces
  the manual `@tasks` array.
- **Per-connection supervisor** — each connection spawns a supervisor
  task (on the socket barrier) that watches for the first pump exit and
  runs `lost!` in `ensure`. Placing the supervisor outside the
  per-connection barrier avoids the self-stop footgun.
- **Connect timeout** — `Transport::TCP.connect` uses
  `Socket.tcp(host, port, connect_timeout:)` instead of `TCPSocket.new`.
  Timeout derived from `reconnect_interval` (floor 0.5s). Fixes macOS
  hang where IPv6 `connect(2)` never delivers `ECONNREFUSED`.
- **Handshake timeout** — SP greeting exchange wrapped in
  `Async::Task#with_timeout(handshake_timeout)`. Prevents a hang when a
  non-NNG service accepts the TCP connection but never sends a greeting.
- **Reconnect after handshake failure** — `ConnectionLifecycle#handshake!`
  now calls `tear_down!(reconnect: true)` on error instead of bare
  `transition!(:closed)`, so the endpoint doesn't go dead when a peer
  RSTs mid-handshake.
- **Quantized reconnect sleeps** — `Reconnect#quantized_wait` aligns
  retries to wall-clock grid boundaries. Multiple clients reconnecting
  with the same interval wake at the same instant.
- **Send pump fairness yield** — `Async::Task.current.yield` after each
  batch write ensures peer pumps get a turn when the queue stays
  non-empty.
- Add `DESIGN.md` documenting the architecture.
- **Versioned socket names** — `PUSH` → `PUSH0`, `PULL` → `PULL0`, etc.
  Canonical names now include the SP protocol version. Unversioned
  aliases (`NNQ::PUSH = NNQ::PUSH0`) are kept for backward compat.
- **`raw:` kwarg** — `Socket#initialize` accepts `raw: false`. Plumbing
  for raw-mode routing (device/proxy support). No functional raw
  routing yet.
- **`NNQ::BUS0`** — best-effort bidirectional mesh (bus0). Fan-out send
  to all peers (drop when full), shared recv queue. Self-pairing.
- **`NNQ::SURVEYOR0` / `NNQ::RESPONDENT0`** — survey/response pattern
  (survey0). Surveyor broadcasts a survey with a timed reply window
  (`options.survey_time`, default 1s). Respondent echoes the backtrace
  like REP. Shared `Routing::Backtrace` module extracted from REP.
- **`NNQ::TimedOut`** error raised when the survey window expires.

## 0.4.0 — 2026-04-09

- `Socket#all_peers_gone` — `Async::Promise` resolving the first time
  the connection set becomes empty after at least one peer connected.
  Edge-triggered, ported from OMQ.
- `Socket#close_read` — closes the recv side only. Buffered messages
  drain, then `#receive` returns `nil`. Send side stays operational.
- `Socket#reconnect_enabled` / `#reconnect_enabled=` — flipped by
  transient-mode consumers before draining to prevent the background
  reconnect loop from revivifying a dying socket.
- `Socket#monitor` / `NNQ::MonitorEvent` — lifecycle event stream
  emitting `:listening`, `:connect_delayed`, `:connect_retried`,
  `:connected`, `:handshake_succeeded`/`_failed`, `:disconnected`,
  `:closed`, and (when `verbose: true`) `:message_sent` /
  `:message_received`. Ported from OMQ, minus the heartbeat/mechanism
  events nnq doesn't have.
- Background reconnect — `NNQ::Engine::Reconnect` runs a `transient: true`
  task per dialed endpoint, retrying with exponential back-off bounded
  by `options.reconnect_interval` (Numeric or Range). `connect` becomes
  non-blocking for `tcp://` and `ipc://`; `inproc://` stays synchronous.
  `CONNECTION_FAILED` / `CONNECTION_LOST` mutable-at-load-time registries
  let plugins append transport-specific error classes.
- `NNQ::PULL#receive` honors `options.read_timeout` via
  `Fiber.scheduler.with_timeout`. Previously the option was declared
  but inert.
- `NNQ.freeze_for_ractors!` — freezes `Engine::CONNECTION_FAILED`,
  `Engine::CONNECTION_LOST`, and `Engine::TRANSPORTS` so NNQ sockets
  can be used from non-main Ractors. Required for nnq-cli's `pipe -P N`
  parallel worker mode.

## 0.3.0 — 2026-04-09

- `Socket#peer_connected` — `Async::Promise` that resolves with the
  first connected peer (or `nil` on close without any peers). Ported
  from OMQ. Held on `SocketLifecycle`, resolved by `ConnectionLifecycle`
  on first `ready!`, and edge-triggered so callers don't need to poll.
- `bench/` — main throughput suite ported from OMQ. Four patterns
  (push/pull, req/rep, pair, pub/sub) across inproc, ipc, and tcp.
  Calibration-driven burst sizing, fastest-of-3 reporting, regression
  report with `--update-readme` to regenerate README tables.

## 0.2.0 — 2026-04-09

- `NNQ::PUB` / `NNQ::SUB` with local prefix filtering (pub0/sub0).
- `NNQ::PAIR` (pair0) — first-pipe-wins exclusive bidirectional channel.
- `NNQ::REQ` / `NNQ::REP` (req0/rep0) cooked-mode request/reply.
- `NNQ::Engine::ConnectionLifecycle` — per-connection state machine
  (`new → handshaking → ready → closed`) consolidating registration,
  teardown ordering, and idempotent loss handling.
- `NNQ::Engine::SocketLifecycle` — socket-level state machine
  (`new → open → closing → closed`) owning the parent task capture and
  close sequencing.
- `NNQ::ConnectionRejected` — raised by routing strategies (e.g. PAIR's
  second peer) to reject a just-handshook connection without exposing
  it to pumps.

## 0.1.0 — 2026-04-09

Initial Phase 1 slice (push0/pull0 over TCP). Requires Ruby >= 4.0.

- `NNQ::Send::Staging` — opportunistic-batching, HWM-free send path.
- `NNQ::Connection`, `NNQ::Engine`, `NNQ::Socket`.
- `NNQ::PUSH` / `NNQ::PULL` with `NNQ::Routing::Push` (round-robin send)
  and `NNQ::Routing::Pull` (unbounded fair-queue receive).
- `NNQ::Transport::TCP`.
- `NNQ::Reactor` — per-process fallback IO thread for non-Async callers.
