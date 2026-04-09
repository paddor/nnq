# frozen_string_literal: true

# PUSH/PULL sustained pipeline throughput.

require_relative "../bench_helper"

BenchHelper.run("PUSH/PULL", dir: __dir__, peer_counts: [1, 3]) do |transport, ep, peers, payload|
  pull = NNQ::PULL.bind(ep)
  ep   = BenchHelper.resolve_endpoint(transport, pull)

  pushes = peers.times.map { NNQ::PUSH.connect(ep) }
  BenchHelper.wait_connected(pushes) unless transport == "inproc"

  begin
    BenchHelper.measure(pull, pushes, payload)
  ensure
    pushes.each(&:close)
    pull.close
  end
end
