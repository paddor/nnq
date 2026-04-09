# frozen_string_literal: true

# PUB/SUB fan-out throughput.
# PUB sends N messages, each SUB receives all N.
# msgs/s = publish rate.

require_relative "../bench_helper"

BenchHelper.run("PUB/SUB", dir: __dir__, peer_counts: [3]) do |transport, ep, peers, payload|
  # PUB drops when its per-peer queue hits send_hwm — crank it high
  # enough that a whole burst fits without dropping so throughput
  # reflects pump capacity, not drop rate.
  pub = NNQ::PUB.bind(ep, send_hwm: 10_000_000)
  ep  = BenchHelper.resolve_endpoint(transport, pub)

  subs = peers.times.map do
    sub = NNQ::SUB.connect(ep)
    sub.subscribe("")
    sub
  end
  BenchHelper.wait_connected(pub, n: peers) unless transport == "inproc"

  burst = ->(k) {
    send_barrier = Async::Barrier.new
    send_barrier.async { k.times { pub.send(payload) } }
    recv_barrier = Async::Barrier.new
    subs.each { |sub| recv_barrier.async { k.times { sub.receive } } }
    recv_barrier.wait
    send_barrier.wait
  }

  begin
    BenchHelper.measure_best_of(payload, &burst)
  ensure
    subs.each(&:close)
    pub.close
  end
end
