# NNQ — Pure Ruby NNG (SP) on Async

Planning doc for NNQ: a pure-Ruby implementation of nng's scalability protocols (SP) using `async` + `io-stream`. Written 2026-04-09.

Related: `omq` (same author, same stack, ZMTP/ZeroMQ wire protocol). NNQ is the nng-philosophy sibling.

## Goals

- **Pure Ruby, no FFI, no C extension.** Same stack as omq: `async`, `io-stream`, `protocol-sp` (new sister gem, analogous to `protocol-zmtp`).
- **Wire-compatible with libnng** on SP v0 protocols over inproc/ipc/tcp.
- **Honor nng's design philosophy where it matters**, ignore it where it costs throughput without buying anything.
- **Significantly faster than libnng** at the common PUSH/PULL / PUB/SUB / REQ/REP benchmarks. Target: 3–6× libnng for single-fiber tight loops (where the staging batcher sees one message at a time and degenerates to "write+flush per message"); 10–25× libnng for multi-fiber workloads, where opportunistic batching kicks in. Landing in the omq (300k msg/s @ 100 B) neighborhood for the multi-fiber case.
- **First-class Async integration.** User wraps in `Async{}`, no background threads, no hidden reactors, no FFI.
- **Thread-safe sockets.** Like omq. Mutex-protected I/O, concurrent senders from multiple fibers/threads are fine.

## Non-goals (v0.x)

- **No HWM.** No internal per-pipe bounded queues with drop/block policy. Backpressure comes from TCP only.
- **No multipart messages.** SP messages are single-frame (header + body). Keeps the wire simple, kills a class of API questions.
- **No transports beyond inproc / ipc / tcp.** No tls, no ws, no websocket, no ztls. Pluggable later.
- **No CURVE / TLS mechanism.** nng's story for this is different (TLS lives in the transport, not the mechanism). Defer.
- **No wildcard socket types.** Skip bus0 and req0/rep0 survey0/respondent0 for v0.1; start with push0/pull0, pair0/pair1, pub0/sub0, then req0/rep0.
- **No reconnect exponential backoff curve tuning.** Just "reconnect on disconnect with a fixed interval" for v0.1.

## Keep from OMQ (stack & patterns)

- Repo layout (`lib/nnq/`, `lib/nnq/routing/`, `lib/nnq/transport/`, `test/`, `bench/`)
- Socket API shape: `NNQ::PUSH.new`, `.bind(url)`, `.connect(url)`, `.send(msg)`, `.receive`, `.close`
- `Engine` / `Options` split
- `Reactor` pattern (per-gem IO-thread fallback for non-Async callers — *not* shared across gems; see open-question resolution below)
- `Transport` registry (`Engine.transports` hash, pluggable)
- Per-connection fiber task tree, supervised by an Engine root task
- `io-stream` for buffered I/O
- YARD docs on all public methods

## Skip from OMQ (things that are ZMTP-specific or HWM-specific)

- Per-pipe `@send_queue` / `@recv_queue` with HWM
- Multipart message handling (`MORE` flag, `Writable#<<` for frames)
- Envelope-stripping REQ / envelope-restoring REP routing (SP uses a stack of 4-byte backtrace IDs instead — simpler)
- `SingleFrame` mixin (all SP messages are single-frame)
- ZMTP greeting / NULL / CURVE mechanisms
- Conflate option (not an nng concept)

---

## Wire protocol

Implemented as a new sister gem `protocol-sp` (parallel to `protocol-zmtp`). **Phase 0 complete (2026-04-09).** Wire format verified against nng source (`src/sp/transport/tcp/tcp.c`) AND end-to-end against `nngcat` itself in both directions (push0 Ruby → pull0 nngcat, push0 nngcat → pull0 Ruby).

- **SP/TCP frame** — **8-byte big-endian length** + body. (PLAN originally said 4-byte; nng uses `uint64_t txlen` — 8 bytes.) Body = protocol-specific header + payload.
- **Per-protocol headers** (still belong in NNQ, not protocol-sp — protocol-sp only handles the framing layer):
  - push0/pull0: no header
  - pair0: no header
  - pair1: 4-byte TTL (hop count)
  - pub0/sub0: no header on pub side; sub filters client-side by prefix (like ZMTP SUB in `protocol-zmtp`)
  - req0: stack of 4-byte request IDs (backtrace); v0 always a single ID set to `0x80000000 | request_id`
  - rep0: echoes req0 stack
  - bus0: 4-byte peer ID (defer)
  - survey0/respondent0: variant of req0 with deadline (defer)
- **Handshake**: 8-byte fixed greeting `00 'S' 'P' 00 <peer-proto:u16-BE> 00 00`. Bytes 6-7 are reserved (must be zero). The peer protocol id uses `NNI_PROTO(major, minor) = (major << 4) | minor`.
- **No mechanism layer**. Once greeting is validated, data frames start.

`protocol-sp` exports:

- `Protocol::SP::Connection` (analogous to `Protocol::ZMTP::Connection`) with `#handshake!`, `#send_message(bytes)`, `#receive_message`, `#write_message(bytes)` + `#flush` (for opportunistic batching), `#write_wire(pre_encoded)` (for fan-out), mutex-protected I/O. Tracks `#last_received_at` for inactivity timeouts.
- `Protocol::SP::Codec::Frame` — `.encode(bytes)` → length-prefixed wire bytes (frozen). `.read_from(io, max_message_size:)`.
- `Protocol::SP::Codec::Greeting` — `.encode(protocol:)` / `.decode(data)`.
- `Protocol::SP::Protocols` — constants for the 8-bit proto IDs: `PUSH_V0=0x50`, `PULL_V0=0x51`, `PUB_V0=0x20`, `SUB_V0=0x21`, `REQ_V0=0x30`, `REP_V0=0x31`, `PAIR_V0=0x10`, `PAIR_V1=0x11`, `SURVEYOR_V0=0x62`, `RESPONDENT_V0=0x63`, `BUS_V0=0x70`. Plus `VALID_PEERS` table and `NAMES` lookup.

---

## Concurrency & send path

This is the architectural heart. The key question: **how do concurrent senders batch into one syscall without an HWM queue?**

### Sending (no background IO thread)

Each `Connection` (one per peer pipe) holds:

- an `IO::Stream` wrapping the socket (`io-stream` buffered writer)
- a `Send::Staging` object (described below) that coalesces concurrent sends

```ruby
def send(bytes)                    # called from any Async fiber
  @staging.commit(bytes)           # blocks until this exact message is flushed
end
```

`Send::Staging` is a small state machine:

```ruby
class Send::Staging
  def initialize(stream)
    @stream   = stream
    @pending  = []                 # [[bytes, promise], ...] - in-memory only
    @draining = false
    @mutex    = Mutex.new          # thread-safety across non-Async callers
  end

  def commit(bytes)
    promise = Async::Promise.new
    @mutex.synchronize { @pending << [bytes, promise] }
    drain                          # tries to become the drainer
    promise.wait                   # blocks this fiber until flushed
  end

  private

  def drain
    return if @draining            # someone else is already draining
    batch = nil
    @mutex.synchronize do
      return if @draining
      @draining = true
      batch = @pending
      @pending = []
    end

    begin
      until batch.empty?
        batch.each { |bytes, _| @stream.write(bytes) }  # coalesced into IO::Stream's write buffer
        @stream.flush                                   # one syscall (writev via io-stream)
        batch.each { |_, promise| promise.resolve(nil) }

        # pick up any messages that arrived during the flush
        @mutex.synchronize do
          batch = @pending
          @pending = []
          @draining = false if batch.empty?
        end
      end
    rescue => e
      batch.each { |_, promise| promise.reject(e) }
      @mutex.synchronize { @draining = false }
      raise
    end
  end
end
```

**Properties:**

- **No HWM.** The `@pending` array is unbounded in principle, but bounded in practice by "how many fibers are concurrently calling `send`". That count is bounded by the user's program structure, not by a configuration knob. In a typical Async program you have tens of concurrent fibers, not millions.
- **Backpressure via blocked flush.** When the OS socket buffer is full, `@stream.flush` blocks (io-stream + Async fiber scheduler → `wait_writable`). New `commit` calls enqueue into `@pending` and then block on their promise, because the drainer is stuck in `flush`. Senders block as soon as TCP pushes back. Identical latency/pressure semantics to libnng's "write directly and block" model.
- **Opportunistic batching falls out for free.** If three fibers each call `commit` in quick succession, the first becomes the drainer, the other two enqueue while the drainer is mid-write, and on the next iteration of the `until batch.empty?` loop the drainer picks up their messages and writev's them in one syscall. No timer, no policy, no HWM.
- **Cancellation = task.stop.** If a fiber blocked on `promise.wait` is cancelled, `Async::Stop` raises through `wait`. A `rescue` clause can look up its entry in `@pending` (if still there and not yet handed to `@stream`) and splice it out. If it's already in `@stream.write_buffer` or already flushed, cancel loses the race — same semantics as libnng's "cancel is best-effort, wins iff not yet committed".
- **Thread-safe.** The `@mutex` guards `@pending` and `@draining` for non-Async callers. Fibers within one thread don't need the mutex but taking it is cheap.

### io-stream flush strategy

Answering the question directly: **don't rely on io-stream's auto-flush**. Its `MINIMUM_WRITE_SIZE = 256 KB` threshold is fine as a *cap* (it means a pathological burst can't run up unbounded RAM), but as a flush *trigger* it's too large for message-oriented throughput — a burst of 10×100-byte messages would sit in the buffer forever without an explicit flush.

The drainer above calls `@stream.flush` **after each batch**, which is both:
- **Correct**: every committed message is guaranteed on-wire before its promise resolves.
- **Coalescing**: multiple `write` calls between flushes go into one underlying `syswrite`/`writev`, so batching is already happening inside io-stream's buffer.

Timer-based flushing is **not needed** and would only add latency. We flush eagerly; the batching is automatic from the producer-consumer interleaving.

### Receiving

Mirror of omq: a per-connection recv fiber reads frames off the stream, pushes decoded messages into a small prefetch buffer, and the user's `receive` call pops from it. No HWM on the recv side either — TCP throttles the sender when the reader falls behind. The recv fiber doesn't need a staging queue because there's only one reader per connection by contract.

v0.1 prefetch buffer size: a `Queue`-ish structure bounded by... actually no, let's start **truly unbounded** on the recv side too, and add a cap only if we hit a concrete problem. The goal is to stay philosophy-consistent with nng: no configured limits, TCP is the limit.

### Cancellation model ("nng aio in Ruby")

nng's aio struct exists because C has no cheap mechanism for "cancellable, awaitable, composable async operation". Ruby with Async does — **every fiber is already that**.

Mapping:
- `nng_aio_alloc + nng_send_aio + nng_aio_wait` → `Async{ socket.send(msg) }.wait`
- `nng_aio_cancel(aio)` → `task.stop`
- `nng_aio_set_timeout(aio, ms)` → `Async::Task.current.with_timeout(seconds) { socket.send(msg) }`
- `nng_aio_result(aio)` → the block's return value / raised exception

No NNQ-specific "AIO" type. `Async::Task` *is* the aio. Document this mapping in the README.

The only subtlety: **"commit result" semantics** for send. When you cancel a send, did the message go out or not? In libnng, the aio result tells you. In NNQ:

- If the fiber is cancelled before `promise.wait` is even entered: message never enqueued, definitely not sent.
- If cancelled while waiting on the promise, and the entry is still in `@pending`: drainer splices it out, definitely not sent.
- If cancelled while the entry is already in `@stream.write_buffer` but not yet flushed: too late, it will be sent (or the whole connection dies). Document as "cancel loses the race, message may or may not be delivered".
- If cancelled after the promise has resolved: the send already succeeded.

This matches nng's "cancel is best-effort, check result" contract exactly. The `Async::Promise` carries the result implicitly.

---

## Subscription matching (pub/sub, sub side)

v0.1: a flat `Hash{String => Set<Connection>}` keyed by subscription prefix, with `String#start_with?` scanning on each incoming message. Acceptable for small subscription counts (tens), linear degradation past that.

v0.2 / plugin: a Rust-backed patricia tree via a tiny new gem (`patricia-tree` or reuse an existing `radix`/`trie` gem — survey first). The matching API is dead simple (`trie.match_prefixes(topic) → [sub_ids]`), so swap is trivial.

Pub side filtering: send to all matching subs. Sub-side filtering (like ZMTP): only if we implement the pub-push-subscriptions-upstream handshake, which nng SUB doesn't do (all subscribers receive all messages by default, then filter locally). So sub-side filtering is the *only* filtering in SP. Even simpler than ZMTP — no need to forward SUBSCRIBE commands over the wire.

---

## Transports

v0.1 scope (same shape as omq's `lib/omq/transport/`):

- `lib/nnq/transport/inproc.rb` — in-process, `NNQ::Transport::Inproc` registry of `@name` → queue pair. Optional DirectPipe optimization for single-peer case.
- `lib/nnq/transport/ipc.rb` — Unix sockets. nng uses a slightly different framing prefix on IPC vs TCP (check nng source for the 1-byte "zero" prefix per message on IPC).
- `lib/nnq/transport/tcp.rb` — TCP with IPv4+IPv6. Same `.bind` / `.connect` shape as omq.

**Transport registry**: reuse the omq pattern (flat hash of scheme → module, frozen on first use, plugins append by assignment). Same API so users familiar with `omq-transport-tls` can write `nnq-transport-tls` later.

---

## Module layout

```
lib/nnq.rb                 # loads everything
lib/nnq/version.rb
lib/nnq/socket.rb          # base class: bind/connect/send/receive/close
lib/nnq/engine.rb          # per-socket orchestrator, task tree, transport dispatch
lib/nnq/options.rb         # (much smaller than omq's: identity, reconnect_interval, read_timeout, write_timeout)
lib/nnq/reactor.rb         # shared IO thread fallback for non-Async callers
lib/nnq/connection.rb      # per-pipe: handshake, send staging, recv loop
lib/nnq/send/staging.rb    # Send::Staging (the core of the send path, described above)
lib/nnq/push_pull.rb       # NNQ::PUSH, NNQ::PULL
lib/nnq/pair.rb            # NNQ::PAIR0, NNQ::PAIR1
lib/nnq/pub_sub.rb         # NNQ::PUB, NNQ::SUB
lib/nnq/req_rep.rb         # NNQ::REQ, NNQ::REP (v0.2)
lib/nnq/routing/push.rb    # round-robin send, fair-queue recv (same shape as omq)
lib/nnq/routing/pull.rb
lib/nnq/routing/pair.rb
lib/nnq/routing/pub.rb
lib/nnq/routing/sub.rb
lib/nnq/routing/req.rb     # v0.2
lib/nnq/routing/rep.rb     # v0.2
lib/nnq/transport/inproc.rb
lib/nnq/transport/ipc.rb
lib/nnq/transport/tcp.rb

# sister gem:
../protocol-sp/lib/protocol/sp/connection.rb
../protocol-sp/lib/protocol/sp/codec/frame.rb
../protocol-sp/lib/protocol/sp/codec/greeting.rb
../protocol-sp/lib/protocol/sp/protocols.rb
```

---

## Phases

Iterative, verify each before moving on (per my `feedback_iterative_phases`).

### Phase 0 — protocol-sp skeleton ✅ done 2026-04-09
1. ✅ Created `protocol-sp` gem at `/home/roadster/dev/oss/omq/protocol-sp/`, zero runtime deps.
2. ✅ Implemented `Codec::Frame` (8-byte BE length + body — corrected from "4-byte LE" in the original plan after reading nng `tcp.c`). Round-trip tested.
3. ✅ Implemented `Codec::Greeting` (8 bytes), exact byte layout `00 'S' 'P' 00 <peer-proto:u16-BE> 00 00`. Round-tripped against all 11 protocol IDs.
4. ✅ Implemented `Protocol::SP::Connection` wrapping an `io-stream`, mutex-protected. `#handshake!` / `#send_message(bytes)` / `#receive_message`, plus `#write_message`/`#flush` and `#write_wire`. Tested against self over `UNIXSocket.pair`.
5. ✅ **Interop test**: push0 Ruby → pull0 nngcat AND push0 nngcat → pull0 Ruby over tcp, both green. Wire format proven.

Test suite: 18 runs / 35 assertions / 0 failures, including 2 nngcat interop tests. Source-of-truth byte-level reference: `nng src/sp/transport/tcp/tcp.c`.

### Phase 1 — NNQ core, push0/pull0 only
1. Copy `omq`'s engine/socket/reactor/options skeleton, strip ZMTP-specific code (multipart, mechanisms, envelopes).
2. Implement `Send::Staging` as described above. Unit-test with a fake stream.
3. `NNQ::PUSH` / `NNQ::PULL` with round-robin / fair-queue routing. Follow omq's routing classes as a template but use SP framing.
4. `Transport::TCP` — smallest possible. Bind, connect, reconnect on disconnect with fixed interval.
5. **Interop**: NNQ::PUSH → libnng PULL and libnng PUSH → NNQ::PULL, both directions, over tcp. Each message's bytes match.
6. **Benchmark**: single-pipe throughput vs libnng + omq + libzmq, 500k msgs @ 100 B / 1 KB. Publish numbers. Target: ≥ 200k msg/s @ 100 B (= ~6× libnng).

### Phase 2 — inproc + ipc
1. `Transport::Inproc` with the DirectPipe optimization (single-peer bypass).
2. `Transport::IPC`. Handle the IPC framing prefix difference from TCP.
3. Interop tests against libnng on IPC.
4. Benchmark on inproc (expect this to be where NNQ shines — no kernel, all Ruby, and the staging batcher has very low overhead).

### Phase 3 — pair0, pair1, pub0/sub0
1. pair0 / pair1 (pair1 adds TTL). Simple routing.
2. pub0 / sub0 with the `Hash{prefix => subs}` matcher.
3. Interop tests.
4. Verify a simple pub/sub demo against `nngcat --sub0`.

### Phase 4 — req0/rep0
1. Request ID stack (4-byte BE, high bit set).
2. REQ retries / timeouts (nng defaults).
3. REP echo of request ID.
4. Interop tests.

### Phase 5 — polish, release 0.1
1. YARD docs pass.
2. README with the "Async::Task is the aio" mapping.
3. `nnq-cli` sibling gem (mirror `omq-cli`).
4. CHANGELOG, gemspec, publish.

### Deferred to 0.2+
- `bus0`, `surveyor0`/`respondent0`
- `Transport::TLS` (as a plugin gem)
- Rust-backed patricia tree subscription matcher
- `nnq-ffi` backend (libnng under the same NNQ API, for A/B testing)
- Raw mode (`socket.raw = true`) for proxies
- Heartbeat / keepalive (nng has pipe-level keepalive, not a ZMTP-style heartbeat command)

---

## Open questions

1. **Where does `options.reconnect_interval` live?** nng's model is per-pipe (actually per-dialer). omq's is per-socket. Match omq for consistency; diverge later if it matters.
2. **`send` while not connected**: nng blocks until a pipe exists. omq also blocks. Match both.
3. **Identity / socket name**: nng has `NNG_OPT_SOCKNAME`, mostly debug. Copy omq's `identity` field, stringly-typed.
4. ~~**Should NNQ's `Engine` reuse omq's shared `Reactor` thread**, or have its own?~~ **Resolved 2026-04-09**: I had confused myself about what `OMQ::Reactor` even is. It's not an Async scheduler — it's just a fallback background thread that runs `Async{ … }` when a non-Async caller needs to use a socket. The actual Async reactor (the `Async::Task` tree, the Fiber::Scheduler) is global to the process already; any task started by NNQ runs in the same scheduler as any task started by OMQ, with messages flowing freely between cooperative fibers. So **no need to share or extract anything**. Each gem keeps its own private `NNQ::Reactor` / `OMQ::Reactor` fallback thread; they coexist trivially. Drop the "extract to async-shared-reactor" idea.
5. **Error class hierarchy**: nng has `NNG_ETIMEDOUT`, `NNG_ECLOSED`, `NNG_ECONNREFUSED`, etc. Map to Ruby exception classes under `NNQ::Error`, mirroring the libnng names for familiarity.
6. **HWM escape hatch**: should `send_queue_limit` be configurable at all, even if default nil? Probably no — commit to "no HWM, ever". If a user wants bounded queueing they can wrap `send` themselves with an Async::Semaphore.

---

## Existing Ruby nng attempts to check before starting

Found in `/home/roadster/dev/oss/`:

- **`nng-ruby`** — unknown, check its architecture. Probably the upstream home of the `nng` gem (C extension, maintained at github.com/paddor/rbnng — same author).
- **`nanowire`** — user's own unified API across nng and zmq. NNQ becomes the pure-Ruby nng backend for nanowire once it exists, replacing the C-ext `nng` gem dependency for users who want no native deps.
- **`io-endpoint-zmtp`** — unclear, check. If it's an `io-endpoint` wrapper speaking ZMTP, there might be a parallel `io-endpoint-sp` worth building for similar reasons.

Quick audit of these before Phase 0 to avoid duplicating work or missing prior art.
