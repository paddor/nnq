# frozen_string_literal: true

# REQ/REP synchronous roundtrip throughput.

require_relative "../bench_helper"

BenchHelper.run("REQ/REP", dir: __dir__, peer_counts: [1]) do |transport, ep, _peers, payload|
  Async do |task|
    rep = NNQ::REP.bind(ep)
    ep  = BenchHelper.resolve_endpoint(transport, rep)
    req = NNQ::REQ.connect(ep)
    BenchHelper.wait_connected(req) unless transport == "inproc"

    responder = task.async do
      loop do
        body = rep.receive
        rep.send_reply(body)
      end
    end

    begin
      BenchHelper.measure_roundtrip(req, responder, payload)
    ensure
      responder.stop
      req.close
      rep.close
    end
  end.wait
end
