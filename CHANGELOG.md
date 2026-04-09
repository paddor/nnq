# Changelog

## 0.1.0 — 2026-04-09

Initial Phase 1 slice (push0/pull0 over TCP). Requires Ruby >= 4.0.

- `NNQ::Send::Staging` — opportunistic-batching, HWM-free send path.
- `NNQ::Connection`, `NNQ::Engine`, `NNQ::Socket`.
- `NNQ::PUSH` / `NNQ::PULL` with `NNQ::Routing::Push` (round-robin send)
  and `NNQ::Routing::Pull` (unbounded fair-queue receive).
- `NNQ::Transport::TCP`.
- `NNQ::Reactor` — per-process fallback IO thread for non-Async callers.
