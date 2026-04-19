# Changelog

## 0.8.0 — 2026-04-19

- **Uniform frozen + `BINARY` message contract across transports.**
  `Socket#coerce_binary` replaces the old `frozen_binary` + `.b.freeze`
  copy on the hot send path. Every send method runs its body through
  `coerce_binary`, which:
  - coerces non-String bodies via `#to_str` (nil / `42` / `:foo` raise
    `NoMethodError` instead of producing a zero-byte frame);
  - re-tags unfrozen non-BINARY bodies to `Encoding::BINARY` in place —
    a flag flip, no copy;
  - freezes the body.

  Receivers always see a frozen BINARY-tagged body: TCP/IPC get it via
  the recv-pump freeze, inproc gets it via `Pipe#send_message`, which
  only allocates for the pathological case of a frozen non-BINARY body
  (the typical `# frozen_string_literal: true` UTF-8 literal). Bodies
  returned by REP/REQ/SURVEYOR/RESPONDENT (cooked and raw) are frozen
  by `parse_backtrace` and the REQ/SURVEYOR id-parsing paths. Mutation
  bugs surface as `FrozenError` instead of silently corrupting a shared
  reference on the inproc fast path. Inproc throughput pays ~20-30%
  for the contract; TCP/IPC unaffected.

- **Benchmarks send fresh strings per iteration.** `BenchHelper.run`
  passes an unfrozen `"x" * size` through to the burst closure; the
  `measure` / `measure_roundtrip` bursts `.dup` it before each send.
  More realistic than reusing one frozen payload and hitting every
  fast path in `coerce_binary` + `Pipe#send_message`.

## 0.7.0 — 2026-04-18

- **Inproc transport now uses a queue-based `Inproc::Pipe`** instead
  of a Unix `socketpair(2)` running the full SP protocol.
  `NNQ::Transport::Inproc::Pipe` duck-types `NNQ::Connection` and
  transfers frozen Strings through a pair of `Async::Queue`s (one
  per direction). No framing, no handshake, no kernel buffer copy.
  When a routing strategy supplies an SP backtrace header
  (REQ/REP/SURVEYOR), it's prepended before enqueue so the receive
  side sees the same layout as the TCP/IPC path and `parse_backtrace`
  keeps working unchanged. The new `Engine#connection_ready(conn,
  endpoint:)` and `ConnectionLifecycle#ready_direct!` entry points
  register a pipe as ready without the SP handshake phase.
- **Inproc direct-recv fast path.** When a routing strategy exposes a
  `#direct_recv_for(conn)` hook, the peer pipe enqueues directly into
  the routing recv queue via `Pipe#wire_direct_recv`, bypassing both
  the intermediate pipe queue and the recv pump fiber. PULL, BUS,
  PAIR, SUB, REP, RESPONDENT, SURVEYOR, and the `*_raw` variants all
  implement the hook; REQ (promise-based) stays on the fiber path.
  Cuts three fiber hops to one on the steady-state recv path.
- **Routing pumps shed their `@pump_tasks` bookkeeping.** `bus`, `pub`,
  `surveyor`, and `surveyor_raw` no longer track per-connection pump
  tasks in a hash. Pumps are spawned under
  `@engine.connections[conn].barrier`, so `ConnectionLifecycle#tear_down!`
  already cascade-cancels them on `barrier.stop` — the hash was dead
  weight.
- **Transport registry is pluggable.** `NNQ::Engine.transports` is now a
  mutable class-level `Hash` instead of a frozen constant; each built-in
  transport (`tcp`, `ipc`, `inproc`) self-registers at load with
  `Engine.transports["…"] = self`. External transports (e.g. `nnq-zstd`'s
  `zstd+tcp://`) can register themselves the same way.
- **`ConnectionLifecycle` calls `transport.wrap_connection(conn, engine)`
  after handshake.** Transports that implement the hook can return a
  delegating wrapper that layers compression / TLS / instrumentation
  over the raw `NNQ::Connection` without the engine caring. Transports
  without the hook (tcp/ipc/inproc) pass through unchanged.
- **`lib/nnq.rb` restructured to mirror `lib/omq.rb`.** Requires split
  into Core / Transport / Socket-types sections. New
  `lib/nnq/constants.rb` owns `MonitorEvent`, the `CONNECTION_LOST` /
  `CONNECTION_FAILED` error arrays, and `NNQ.freeze_for_ractors!` — all
  previously scattered across `engine.rb`, `reconnect.rb`,
  `monitor_event.rb`, and the top-level `nnq.rb`. `monitor_event.rb` is
  removed (absorbed into constants).
- **Benchmarks: richer scaffolding, measured via `Async::Clock`.**
  `BenchHelper` gains `NNQ_BENCH_SIZES` / `NNQ_BENCH_TRANSPORTS` /
  `NNQ_BENCH_PEERS` env overrides, a `measure_roundtrip` helper for
  REQ/REP-style patterns, and a `wait_subscribed` helper that closes
  the gap between TCP connect and SUBSCRIBE propagation. All elapsed
  measurements use `Async::Clock.measure { … }` blocks instead of
  `Process.clock_gettime`. `bench/report.rb --update-readme` now
  falls back to the most recent row per cell across all history, so a
  partial bench run refreshes only the cells it covers instead of
  clobbering untouched cells with "—".

## 0.6.1 — 2026-04-15

- **Verbose trace (`-vvv`) now fires for cooked REQ/REP/RESPONDENT
  sends.** Cooked `Req#send_request`, `Rep#send_reply`, and
  `Respondent#send_reply` bypass `send_pump` and write to the
  connection directly, so they were never emitting `:message_sent`
  monitor events — `-vvv` only ever showed the `<<` recv side. Each
  now calls `emit_verbose_msg_sent(body)` after the write. Raw
  REQ/REP/RESPONDENT sends get the same treatment (raw surveyor
  already emitted via its per-peer send pump).
- **Verbose recv previews strip the SP backtrace header.** The recv
  loop used to emit the raw wire body, so `-vvv` traces for
  REQ/REP/SURVEYOR/RESPONDENT showed the 4-byte request/survey id
  (or a multi-word backtrace stack) in front of the payload. Routing
  strategies now expose an optional `preview_body(wire)` hook; the
  engine calls it before emitting `:message_received` so the trace
  shows just the payload.
- **`Engine#close` drains the monitor queue before cancelling
  tasks.** The monitor consumer fiber lives under the socket-level
  barrier, so `barrier.stop` used to `Async::Stop` it before it had
  a chance to drain trailing events. `close` now emits `:closed`,
  enqueues the nil sentinel, and awaits the stored `monitor_task`
  before stopping the barrier. Fixes flaky `-vvv` traces on
  short-lived sockets where the last `:message_received` event
  would occasionally be lost.

## 0.6.0 — 2026-04-15

- **NNG-style raw mode for REQ/REP and SURVEYOR/RESPONDENT.** Constructing
  any of the four with `raw: true` bypasses the cooked state machine
  (request-id tracking, pending-reply slot, survey window) and exposes
  the full SP backtrace header as an opaque, caller-supplied handle.
  - `#receive` returns `[pipe, header, body]` where `pipe` is the live
    `NNQ::Connection` that delivered the message (idiomatic Ruby handle
    — no opaque pipe_id token, no lookup registry), `header` is the
    parsed backtrace bytes, and `body` is the payload.
  - Raw REQ/SURVEYOR send: `send(body, header:)` — fans round-robin /
    fans out.
  - Raw REP/RESPONDENT send: `send(body, to:, header:)` — routes
    directly to a prior `pipe` with the stored `header` written
    verbatim, so the cooked peer matches the reply. Closed peer or
    over-TTL header → silent drop (matches NNG behavior).
  - Cooked-mode methods (`send_request`, `send_reply`, `send_survey`)
    raise `NNQ::Error` in raw mode and vice versa.
  - Unblocks proxy/device-style use cases (forwarders, request routers)
    without touching the cooked code paths. `lib/nnq/routing/{req,rep,
    surveyor,respondent}_raw.rb` live alongside their cooked siblings;
    `build_routing` branches on `@raw` inside REQ0/REP0/SURVEYOR0/
    RESPONDENT0. PUB/SUB and PUSH/PULL raw are still out of scope.
- **Zero-alloc cooked send paths via protocol-sp `header:` kwarg.**
  `Connection#send_message` / `#write_message` grow an optional
  `header:` kwarg that protocol-sp writes between the SP length prefix
  and the body as a third buffered write (coalesced into a single
  `writev`). Cooked `Req#send_request`, `Rep#send_reply`, and
  `Respondent#send_reply` no longer allocate the `header + body`
  intermediate String on every send — the savings apply to every
  REQ/REP round trip regardless of whether raw mode is used.
  Requires `protocol-sp >= 0.3`.
- **`Options#recv_hwm`** — new option, defaults to `Options::DEFAULT_HWM`
  (same as `send_hwm`). Bounds the raw routing strategies' receive
  queues; the cooked paths still use their existing (unbounded) state
  and are unaffected.

## 0.5.0 — 2026-04-15

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
