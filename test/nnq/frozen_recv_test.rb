# frozen_string_literal: true

require_relative "../test_helper"
require "tmpdir"
require "securerandom"

describe "received body is frozen" do
  def assert_frozen_body(body)
    assert body.is_a?(String), "expected String, got #{body.class}"
    assert body.frozen?, "expected body to be frozen"
    assert_equal Encoding::BINARY, body.encoding, "expected body BINARY-tagged"
  end


  [
    ["inproc", -> { "inproc://frozen-#{SecureRandom.hex(4)}" }],
    ["ipc",    -> { "ipc://#{Dir.tmpdir}/nnq-frozen-#{SecureRandom.hex(4)}.sock" }],
    ["tcp",    -> { "tcp://127.0.0.1:0" }],
  ].each do |transport, ep_builder|
    it "freezes PUSH/PULL bodies over #{transport}" do
      Sync do
        pull = NNQ::PULL.bind(ep_builder.call)
        push = NNQ::PUSH.connect(pull.last_endpoint)
        push.peer_connected.wait unless transport == "inproc"

        push.send("hello")
        body = pull.receive

        assert_equal "hello", body
        assert_frozen_body(body)
      ensure
        push&.close
        pull&.close
      end
    end


    it "freezes REQ/REP cooked bodies over #{transport}" do
      Sync do
        rep = NNQ::REP.bind(ep_builder.call)
        req = NNQ::REQ.connect(rep.last_endpoint)
        req.peer_connected.wait unless transport == "inproc"

        responder = Async do
          request = rep.receive
          assert_frozen_body(request)
          rep.send_reply("pong")
        end

        reply = req.send_request("ping")
        responder.wait
        assert_frozen_body(reply)
      ensure
        req&.close
        rep&.close
      end
    end
  end


  it "upgrades frozen non-BINARY parts to BINARY on inproc" do
    Sync do
      pull = NNQ::PULL.bind("inproc://frozen-nonbin-#{SecureRandom.hex(4)}")
      push = NNQ::PUSH.connect(pull.last_endpoint)

      frozen_utf8 = "héllo".freeze
      assert frozen_utf8.frozen?
      refute_equal Encoding::BINARY, frozen_utf8.encoding

      push.send(frozen_utf8)
      body = pull.receive

      assert_frozen_body(body)
      assert_equal frozen_utf8.bytes, body.bytes
      refute_equal Encoding::BINARY, frozen_utf8.encoding,
        "caller's string encoding must not be mutated"
    ensure
      push&.close
      pull&.close
    end
  end


  it "coerces String-like parts via #to_str" do
    Sync do
      pull = NNQ::PULL.bind("inproc://to_str-#{SecureRandom.hex(4)}")
      push = NNQ::PUSH.connect(pull.last_endpoint)

      stringy = Class.new do
        def initialize(s)
          @s = s
        end


        def to_str
          @s
        end

      end

      push.send(stringy.new("wrapped"))
      body = pull.receive

      assert_frozen_body(body)
      assert_equal "wrapped", body
    ensure
      push&.close
      pull&.close
    end
  end


  it "raises on non-String-like parts (including nil)" do
    Sync do
      pull = NNQ::PULL.bind("inproc://bad-part-#{SecureRandom.hex(4)}")
      push = NNQ::PUSH.connect(pull.last_endpoint)

      assert_raises(NoMethodError) { push.send(42) }
      assert_raises(NoMethodError) { push.send(:sym) }
      assert_raises(NoMethodError) { push.send(nil) }
    ensure
      push&.close
      pull&.close
    end
  end


  it "re-tags unfrozen non-BINARY parts in place via #send" do
    Sync do
      pull = NNQ::PULL.bind("inproc://unfrozen-nonbin-#{SecureRandom.hex(4)}")
      push = NNQ::PUSH.connect(pull.last_endpoint)

      utf8 = String.new("héllo", encoding: Encoding::UTF_8)
      refute utf8.frozen?
      refute_equal Encoding::BINARY, utf8.encoding

      push.send(utf8)
      body = pull.receive

      assert_frozen_body(body)
      assert utf8.frozen?, "expected caller's string to be frozen after send"
      assert_equal Encoding::BINARY, utf8.encoding, "expected caller's string to be BINARY-tagged after send"
    ensure
      push&.close
      pull&.close
    end
  end

end
