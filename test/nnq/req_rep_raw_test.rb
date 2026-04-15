# frozen_string_literal: true

require_relative "../test_helper"

describe "REQ/REP raw mode" do
  it "round-trips raw REQ <-> cooked REP (REQ hand-packs id)" do
    Sync do |task|
      rep = NNQ::REP.bind("tcp://127.0.0.1:0")
      req = NNQ::REQ.new(raw: true)
      req.connect(rep.last_endpoint)

      server = task.async do
        body = rep.receive
        rep.send_reply("echo: #{body}")
      end

      id_bytes = [0xC0FFEE_42 | 0x80000000].pack("N")
      req.send("hello", header: id_bytes)

      pipe, header, reply = req.receive
      refute_nil pipe
      assert_equal id_bytes, header
      assert_equal "echo: hello", reply

      server.wait
    ensure
      req&.close
      rep&.close
    end
  end


  it "round-trips cooked REQ <-> raw REP (app echoes header verbatim)" do
    Sync do |task|
      rep = NNQ::REP.new(raw: true)
      rep.bind("tcp://127.0.0.1:0")
      req = NNQ::REQ.connect(rep.last_endpoint)

      server = task.async do
        pipe, header, payload = rep.receive
        rep.send("echo: #{payload}", to: pipe, header: header)
      end

      reply = req.send_request("hello")
      assert_equal "echo: hello", reply
      server.wait
    ensure
      req&.close
      rep&.close
    end
  end


  it "raw REP silently drops reply when pipe is closed" do
    Sync do |task|
      rep = NNQ::REP.new(raw: true)
      rep.bind("tcp://127.0.0.1:0")
      req = NNQ::REQ.connect(rep.last_endpoint)

      pipe_ref = nil
      server = task.async do
        pipe, header, _payload = rep.receive
        pipe_ref = pipe
        pipe.close
        rep.send("should-not-arrive", to: pipe, header: header)
      end

      assert_raises(Async::TimeoutError) do
        Fiber.scheduler.with_timeout(0.2) { req.send_request("hi") }
      end
      server.wait
      refute_nil pipe_ref
    ensure
      req&.close
      rep&.close
    end
  end


  it "raw REQ supports many outstanding requests" do
    n = 10
    Sync do |task|
      rep = NNQ::REP.bind("tcp://127.0.0.1:0")
      req = NNQ::REQ.new(raw: true)
      req.connect(rep.last_endpoint)

      server = task.async do
        n.times do
          body = rep.receive
          rep.send_reply(body.reverse)
        end
      end

      sent_ids = n.times.map do |i|
        id_bytes = [(0x1000 + i) | 0x80000000].pack("N")
        req.send("msg#{i}", header: id_bytes)
        id_bytes
      end

      received = n.times.map { req.receive }
      received_ids  = received.map { |(_pipe, header, _body)| header }
      received_bodies = received.map { |(_pipe, _header, body)| body }

      assert_equal sent_ids.sort, received_ids.sort
      assert_equal n.times.map { |i| "msg#{i}".reverse }.sort, received_bodies.sort
      server.wait
    ensure
      req&.close
      rep&.close
    end
  end


  it "cooked methods raise in raw mode and vice versa" do
    Sync do
      raw_req    = NNQ::REQ.new(raw: true)
      raw_rep    = NNQ::REP.new(raw: true)
      cooked_req = NNQ::REQ.new
      cooked_rep = NNQ::REP.new

      raw_req.bind("tcp://127.0.0.1:0")
      raw_rep.bind("tcp://127.0.0.1:0")
      cooked_req.bind("tcp://127.0.0.1:0")
      cooked_rep.bind("tcp://127.0.0.1:0")

      assert_raises(NNQ::Error) { raw_req.send_request("x") }
      assert_raises(NNQ::Error) { raw_rep.send_reply("x") }
      assert_raises(NNQ::Error) { cooked_req.send("x", header: "abcd") }
      assert_raises(NNQ::Error) { cooked_req.receive }
      assert_raises(NNQ::Error) { cooked_rep.send("x", to: :dummy, header: "abcd") }
    ensure
      raw_req&.close
      raw_rep&.close
      cooked_req&.close
      cooked_rep&.close
    end
  end


  it "proxies cooked REQ -> raw REP -> raw REQ -> cooked REP end to end" do
    Sync do |task|
      backend = NNQ::REP.bind("tcp://127.0.0.1:0")
      proxy_front = NNQ::REP.new(raw: true)
      proxy_front.bind("tcp://127.0.0.1:0")
      proxy_back = NNQ::REQ.new(raw: true)
      proxy_back.connect(backend.last_endpoint)
      client = NNQ::REQ.connect(proxy_front.last_endpoint)

      backend_task = task.async do
        body = backend.receive
        backend.send_reply("reply: #{body}")
      end

      pending = {}
      forward = task.async do
        pipe_in, header_in, body = proxy_front.receive
        pending[header_in] = pipe_in
        proxy_back.send(body, header: header_in)
      end

      return_task = task.async do
        _pipe_back, header_back, reply = proxy_back.receive
        pipe_in = pending.delete(header_back)
        proxy_front.send(reply, to: pipe_in, header: header_back)
      end

      result = client.send_request("hello")
      assert_equal "reply: hello", result

      [forward, return_task, backend_task].each(&:wait)
    ensure
      client&.close
      proxy_back&.close
      proxy_front&.close
      backend&.close
    end
  end
end
