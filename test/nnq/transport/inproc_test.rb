# frozen_string_literal: true

require_relative "../../test_helper"

describe NNQ::Transport::Inproc do
  before do
    NNQ::Transport::Inproc.reset!
  end


  it "delivers PUSH/PULL over inproc" do
    Sync do
      pull = NNQ::PULL.bind("inproc://push-pull")
      push = NNQ::PUSH.connect("inproc://push-pull")
      push.send("alpha")
      push.send("beta")
      assert_equal "alpha", pull.receive
      assert_equal "beta",  pull.receive
    ensure
      push&.close
      pull&.close
    end
  end


  it "delivers many messages over inproc" do
    n = 1_000
    Sync do
      pull = NNQ::PULL.bind("inproc://bulk")
      push = NNQ::PUSH.connect("inproc://bulk")
      sender = Async do
        n.times { |i| push.send("msg-#{i}") }
      end
      n.times { |i| assert_equal "msg-#{i}", pull.receive }
      sender.wait
    ensure
      push&.close
      pull&.close
    end
  end


  it "round-trips PAIR over inproc" do
    Sync do
      a = NNQ::PAIR.bind("inproc://pair")
      b = NNQ::PAIR.connect("inproc://pair")
      a.send("ping")
      assert_equal "ping", b.receive
      b.send("pong")
      assert_equal "pong", a.receive
    ensure
      a&.close
      b&.close
    end
  end


  it "round-trips REQ/REP over inproc" do
    Sync do
      rep = NNQ::REP.bind("inproc://rr")
      req = NNQ::REQ.connect("inproc://rr")
      task = Async do
        body = rep.receive
        rep.send_reply(body.upcase)
      end
      assert_equal "HELLO", req.send_request("hello")
      task.wait
    ensure
      req&.close
      rep&.close
    end
  end


  it "raises when connecting before bind" do
    Sync do
      assert_raises(NNQ::Error) { NNQ::PUSH.connect("inproc://unbound") }
    end
  end


  it "raises on double bind" do
    Sync do
      pull1 = NNQ::PULL.bind("inproc://dup")
      assert_raises(NNQ::Error) { NNQ::PULL.bind("inproc://dup") }
    ensure
      pull1&.close
    end
  end


  it "allows rebind after close" do
    Sync do
      pull1 = NNQ::PULL.bind("inproc://reuse")
      pull1.close
      pull2 = NNQ::PULL.bind("inproc://reuse")
      push  = NNQ::PUSH.connect("inproc://reuse")
      push.send("ok")
      assert_equal "ok", pull2.receive
    ensure
      push&.close
      pull2&.close
    end
  end
end
