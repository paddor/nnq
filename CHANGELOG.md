# Changelog

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
