# NNQ — pure Ruby NNG on Async

[![CI](https://github.com/paddor/nnq/actions/workflows/ci.yml/badge.svg)](https://github.com/paddor/nnq/actions/workflows/ci.yml)
[![Gem Version](https://img.shields.io/gem/v/nnq?color=e9573f)](https://rubygems.org/gems/nnq)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-%3E%3D%204.0-CC342D?logo=ruby&logoColor=white)](https://www.ruby-lang.org)

NNQ is a pure-Ruby implementation of nanomsg's Scalability Protocols
(SP), wire-compatible with libnng. It is the nng-philosophy sibling of
[omq](https://github.com/paddor/omq) (pure-Ruby ZeroMQ).

Status: pre-alpha. v0.0.1 implements push0/pull0 over TCP only. See
[PLAN.md](PLAN.md) for the design and roadmap.

## Why a pure-Ruby NNG?

- **No native deps.** Same stack as omq: `async`, `io-stream`,
  `protocol-sp`. No C extension, no FFI.
- **Faster than libnng** for the multi-fiber case (target: 10–25×).
  libnng's per-message aio model leaves all the throughput of write
  coalescing on the table.
- **Async-native.** Wrap in `Async{}`, no background thread for users
  who already run a reactor.

## Quickstart

```ruby
require "nnq"
require "async"

Async do
  pull = NNQ::PULL.bind("tcp://127.0.0.1:5570")
  push = NNQ::PUSH.connect("tcp://127.0.0.1:5570")

  push.send("hello")
  puts pull.receive  # => "hello"

  push.close
  pull.close
end
```

## The "no HWM" philosophy

NNQ has no high-water mark. Backpressure comes only from the kernel
socket buffer. Concurrent senders are coalesced into one writev syscall
by `NNQ::Send::Staging`, which keeps a tiny in-memory aggregation point
that exists only during the moments when the drainer fiber is parked
on `wait_writable`. See `lib/nnq/send/staging.rb` for the full
discussion.

## Cancellation

`Async::Task` is the aio. To cancel an in-flight send, stop the task
that called `send`. The semantics match libnng's
`nng_aio_cancel` exactly: best-effort, may lose the race if the message
is already on the wire.
