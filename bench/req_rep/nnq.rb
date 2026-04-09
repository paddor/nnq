# frozen_string_literal: true

# REQ/REP synchronous roundtrip throughput.

require_relative "../bench_helper"

BenchHelper.run("REQ/REP", dir: __dir__, peer_counts: [1]) do |transport, ep, _peers, payload|
  Async do |task|
    rep = NNQ::REP.bind(ep)
    ep  = BenchHelper.resolve_endpoint(transport, rep)
    req = NNQ::REQ.connect(ep)

    responder = task.async do
      loop do
        body = rep.receive
        rep.send_reply(body)
      end
    end

    burst = ->(k) { k.times { req.send_request(payload) } }

    begin
      BenchHelper.measure_best_of(payload, &burst)
    ensure
      responder.stop
      req.close
      rep.close
    end
  end.wait
end
