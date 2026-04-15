# frozen_string_literal: true

require_relative "../test_helper"

describe "SURVEYOR/RESPONDENT raw mode" do
  it "round-trips raw SURVEYOR <-> cooked RESPONDENT" do
    Sync do
      surveyor   = NNQ::SURVEYOR0.new(raw: true)
      surveyor.bind("tcp://127.0.0.1:0")
      respondent = NNQ::RESPONDENT0.connect(surveyor.last_endpoint)

      surveyor.peer_connected.wait

      id_bytes = [0x1234_5678 | 0x80000000].pack("N")
      surveyor.send("ping", header: id_bytes)

      body = respondent.receive
      assert_equal "ping", body
      respondent.send_reply("pong")

      pipe, header, reply = surveyor.receive
      refute_nil pipe
      assert_equal id_bytes, header
      assert_equal "pong", reply
    ensure
      respondent&.close
      surveyor&.close
    end
  end


  it "round-trips cooked SURVEYOR <-> raw RESPONDENT" do
    Sync do |task|
      surveyor   = NNQ::SURVEYOR0.bind("tcp://127.0.0.1:0")
      respondent = NNQ::RESPONDENT0.new(raw: true)
      respondent.connect(surveyor.last_endpoint)

      surveyor.peer_connected.wait

      server = task.async do
        pipe, header, body = respondent.receive
        respondent.send("echo: #{body}", to: pipe, header: header)
      end

      surveyor.send_survey("hi")
      reply = surveyor.receive
      assert_equal "echo: hi", reply
      server.wait
    ensure
      respondent&.close
      surveyor&.close
    end
  end


  it "raw SURVEYOR has no survey window (blocks past cooked survey_time)" do
    Sync do |task|
      surveyor   = NNQ::SURVEYOR0.new(raw: true)
      surveyor.bind("tcp://127.0.0.1:0")
      respondent = NNQ::RESPONDENT0.connect(surveyor.last_endpoint)

      surveyor.peer_connected.wait

      id_bytes = [0xDEAD_BEEF | 0x80000000].pack("N")
      surveyor.send("q", header: id_bytes)

      # Respondent answers after longer than cooked survey_time (1s).
      late = task.async do
        respondent.receive
        sleep 1.2
        respondent.send_reply("late")
      end

      pipe, header, reply = surveyor.receive
      refute_nil pipe
      assert_equal id_bytes, header
      assert_equal "late", reply
      late.wait
    ensure
      respondent&.close
      surveyor&.close
    end
  end


  it "raw RESPONDENT routes replies to the right pipe with multiple peers" do
    Sync do |task|
      s1 = NNQ::SURVEYOR0.bind("tcp://127.0.0.1:0")
      s2 = NNQ::SURVEYOR0.bind("tcp://127.0.0.1:0")
      respondent = NNQ::RESPONDENT0.new(raw: true)
      respondent.connect(s1.last_endpoint)
      respondent.connect(s2.last_endpoint)

      50.times do
        break if respondent.connection_count >= 2
        sleep 0.01
      end

      server = task.async do
        2.times do
          pipe, header, body = respondent.receive
          respondent.send("ack: #{body}", to: pipe, header: header)
        end
      end

      s1.send_survey("from-s1")
      s2.send_survey("from-s2")

      assert_equal "ack: from-s1", s1.receive
      assert_equal "ack: from-s2", s2.receive
      server.wait
    ensure
      respondent&.close
      s1&.close
      s2&.close
    end
  end


  it "cooked methods raise in raw mode and vice versa" do
    Sync do
      raw_s        = NNQ::SURVEYOR0.new(raw: true)
      raw_r        = NNQ::RESPONDENT0.new(raw: true)
      cooked_s     = NNQ::SURVEYOR0.new
      cooked_r     = NNQ::RESPONDENT0.new

      raw_s.bind("tcp://127.0.0.1:0")
      raw_r.bind("tcp://127.0.0.1:0")
      cooked_s.bind("tcp://127.0.0.1:0")
      cooked_r.bind("tcp://127.0.0.1:0")

      assert_raises(NNQ::Error) { raw_s.send_survey("x") }
      assert_raises(NNQ::Error) { raw_r.send_reply("x") }
      assert_raises(NNQ::Error) { cooked_s.send("x", header: "abcd") }
      assert_raises(NNQ::Error) { cooked_r.send("x", to: :dummy, header: "abcd") }
    ensure
      raw_s&.close
      raw_r&.close
      cooked_s&.close
      cooked_r&.close
    end
  end


  it "proxies cooked SURVEYOR -> raw RESPONDENT -> raw SURVEYOR -> cooked RESPONDENT" do
    Sync do |task|
      backend_r    = NNQ::RESPONDENT0.new
      proxy_front  = NNQ::RESPONDENT0.new(raw: true)
      proxy_front.bind("tcp://127.0.0.1:0")
      proxy_back   = NNQ::SURVEYOR0.new(raw: true)
      proxy_back.bind("tcp://127.0.0.1:0")
      backend_r.connect(proxy_back.last_endpoint)
      client       = NNQ::SURVEYOR0.connect(proxy_front.last_endpoint)

      proxy_front.peer_connected.wait
      proxy_back.peer_connected.wait

      backend_task = task.async do
        body = backend_r.receive
        backend_r.send_reply("reply: #{body}")
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

      client.send_survey("ping")
      assert_equal "reply: ping", client.receive
      [forward, return_task, backend_task].each(&:wait)
    ensure
      client&.close
      backend_r&.close
      proxy_back&.close
      proxy_front&.close
    end
  end
end
