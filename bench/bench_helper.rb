# frozen_string_literal: true

# Shared scaffolding for per-pattern throughput benchmarks.
#
# Usage:
#   require_relative '../bench_helper'
#   BenchHelper.run("PUSH/PULL", dir: __dir__) do |transport, ep, peers, payload|
#     # Set up sockets, measure, return { mbps:, msgs_s: }
#   end

$VERBOSE = nil
$stdout.sync = true

require "bundler/setup"
require_relative "../lib/nnq"
require "async"
require "async/clock"
require "console"
require "json"
require "set"
Console.logger = Console::Logger.new(Console::Output::Null.new)

module BenchHelper
  # ×4 geometric sweep from 128 B to 32 KiB.
  SIZES = (ENV["NNQ_BENCH_SIZES"] || "128,512,2048,8192,32768").split(",").map(&:to_i).freeze

  # Each cell runs ROUNDS timed rounds and reports the fastest one.
  # Transient jitter (GC, scheduler preemption, YJIT tier-up, kernel
  # batching gaps) only ever *slows* a run down, so "fastest" is the
  # closest approximation to peak sustainable throughput.
  ROUNDS         = 1
  ROUND_DURATION = Float(ENV.fetch("NNQ_BENCH_TARGET", 1.0))

  # Calibration warmup window — long enough that a single scheduler
  # hiccup or YJIT compilation pause doesn't halve the rate estimate.
  WARMUP_DURATION = 0.3

  # Lower bound on warmup iterations (so noisy short bursts don't fool
  # the rate estimate).
  WARMUP_MIN_ITERS = 1_000

  # Iterations of the untimed prime burst that runs before calibration.
  # Soaks up YJIT compilation, fiber stack allocation, kernel buffer
  # ramp-up, etc., so the timed warmup measures steady-state throughput.
  PRIME_ITERS = 1000

  RESULTS_PATH = File.join(__dir__, "results.jsonl").freeze

  module_function

  KERNEL = `uname -r`.strip.freeze

  # Transports under test:
  #   inproc — in-process upper bound
  #   ipc    — Unix-domain sockets, no TCP stack
  #   tcp    — primary networked path
  TRANSPORTS = (ENV["NNQ_BENCH_TRANSPORTS"] || "inproc,ipc,tcp").split(",").freeze

  def run_id
    @run_id ||= ENV["NNQ_BENCH_RUN_ID"] || Time.now.strftime("%Y-%m-%dT%H:%M:%S")
  end

  # Per-size timeout in seconds. Each cell does prime + calibration +
  # ROUNDS × ROUND_DURATION, roughly 4-5s; 30s leaves headroom for the
  # slowest cells (TCP 32 KiB under load).
  RUN_TIMEOUT = Integer(ENV.fetch("NNQ_BENCH_TIMEOUT", 30))

  def run(label, dir:, peer_counts: [1, 3], &block)
    peer_counts = ENV["NNQ_BENCH_PEERS"].split(",").map(&:to_i) if ENV["NNQ_BENCH_PEERS"]
    pattern = File.basename(dir)
    jit     = defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled? ? "+YJIT" : "no JIT"
    puts "#{label} | NNQ #{NNQ::VERSION} | Ruby #{RUBY_VERSION} (#{jit}) | #{KERNEL}"
    puts

    seq = 0

    TRANSPORTS.each do |transport|
      peer_counts.each do |peers|
        header = "#{transport} (#{peers} peer#{'s' if peers > 1})"
        puts "--- #{header} ---"
        completed = 0
        SIZES.each do |size|
          seq += 1
          Async do |task|
            task.with_timeout(RUN_TIMEOUT) do
              NNQ::Transport::Inproc.reset! if transport == "inproc"
              ep = endpoint(transport, seq)
              r  = block.call(transport, ep, peers, "x" * size)
              append_result(pattern, transport, peers, size, r[:n], r[:elapsed], r[:mbps], r[:msgs_s])
              completed += 1
            end
          rescue Async::TimeoutError
            abort "BENCH TIMEOUT: #{header} #{size}B exceeded #{RUN_TIMEOUT}s"
          end
        end
        if completed == 0
          abort "BENCH FAILED: #{header} produced no results"
        end
        puts
      end
    end
  end

  def endpoint(transport, seq)
    case transport
    when "inproc"
      "inproc://bench-#{seq}"
    when "ipc"
      "ipc://@nnq-bench-#{seq}"
    when "tcp"
      "tcp://127.0.0.1:0"
    end
  end

  # Returns the resolved endpoint after bind. nnq's listener captures
  # the real port during bind, so `last_endpoint` is authoritative.
  def resolve_endpoint(_transport, socket)
    socket.last_endpoint
  end

  # Calibrates `n` so that one timed burst lasts ~ROUND_DURATION. The
  # block must transport exactly `k` messages in the same pipelined/
  # burst shape as the real measurement — single-shot ping-pong warmups
  # undercount transports that benefit from batching (TCP, IPC).
  #
  # Doubles a timed burst until it reaches WARMUP_DURATION, then
  # extrapolates to ROUND_DURATION. Caller is responsible for priming
  # (running PRIME_ITERS untimed) before invoking this.
  def estimate_n(target: ROUND_DURATION, warmup: WARMUP_DURATION)
    n = WARMUP_MIN_ITERS
    loop do
      elapsed = Async::Clock.measure { yield n }
      if elapsed >= warmup
        rate = n / elapsed
        return [(rate * target).to_i, WARMUP_MIN_ITERS].max
      end
      n *= 4
    end
  end

  # Runs ROUNDS timed bursts of `n` messages each and reports the
  # fastest one. `align` is an optional integer the caller uses to
  # round `n` to a multiple of its burst shape (e.g. peer count) so
  # per-sender divisions stay even. The block is the burst closure.
  def measure_best_of(payload, align: 1, &burst)
    burst.call(PRIME_ITERS)
    n = estimate_n(&burst)
    n = [(n / align) * align, align].max

    best = nil
    ROUNDS.times do
      elapsed = Async::Clock.measure { burst.call(n) }
      best = elapsed if best.nil? || elapsed < best
    end

    report(payload.bytesize, n, best)
  end

  def measure(receiver, senders, payload)
    burst = ->(k) {
      per     = [k / senders.size, 1].max
      barrier = Async::Barrier.new

      senders.each do |sender|
        barrier.async do
          per.times { sender.send(payload.dup) }
        end
      end

      (per * senders.size).times do
        receiver.receive
      end

      barrier.wait
    }

    measure_best_of(payload, align: senders.size, &burst)
  end


  # REQ/REP or PAIR-as-roundtrip measurement: each iteration does one
  # send+receive on +requester+. Caller owns the responder task.
  def measure_roundtrip(requester, _responder_task, payload)
    burst = ->(k) { k.times { requester.send_request(payload.dup) } }
    measure_best_of(payload, &burst)
  end

  def report(msg_size, n, elapsed)
    mbps   = n * msg_size / elapsed / 1_000_000.0
    msgs_s = n / elapsed
    printf "  %6s  %8.1f MB/s  %8.0f msg/s  (%.2fs, n=%d)\n",
           "#{msg_size}B", mbps, msgs_s, elapsed, n
    { n: n, elapsed: elapsed, mbps: mbps, msgs_s: msgs_s }
  end

  # Waits until each of +sockets+ has its first connected peer via
  # their `peer_connected` promise. Each socket's promise tracks its
  # own first pipe, so this works for both sides: waiting on a single
  # bound socket with multiple dialers means polling, whereas waiting
  # on each dialer's own promise is edge-triggered.
  def wait_connected(*sockets)
    sockets.flatten.each { |s| s.peer_connected.wait }
  end


  # Waits until every SUB has an active subscription at the PUB by
  # sending empty probe messages until each sub receives one. Closes
  # the gap between `peer_connected` (TCP up) and SUBSCRIBE message
  # propagation (prefix table installed on the PUB side).
  def wait_subscribed(pub, subs)
    pending = subs.to_set
    until pending.empty?
      pub.send("")
      pending.each do |sub|
        begin
          Async::Task.current.with_timeout(0.01) { sub.receive }
          pending.delete(sub)
        rescue Async::TimeoutError
          # subscription not yet propagated
        end
      end
    end
  end

  def append_result(pattern, transport, peers, msg_size, msg_count, elapsed, mbps, msgs_s)
    row = {
      run_id:    run_id,
      pattern:   pattern,
      transport: transport,
      peers:     peers,
      msg_size:  msg_size,
      msg_count: msg_count,
      elapsed_s: elapsed.round(6),
      mbps:      mbps.round(2),
      msgs_s:    msgs_s.round(1),
    }
    File.open(RESULTS_PATH, "a") { |f| f.puts(JSON.generate(row)) }
  end
end
