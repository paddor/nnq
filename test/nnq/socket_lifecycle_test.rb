# frozen_string_literal: true

require_relative "../test_helper"

describe "Socket lifecycle extras" do
  describe "#peer_connected" do
    it "resolves when the first peer connects" do
      Sync do
        pull = NNQ::PULL.bind("tcp://127.0.0.1:0")
        push = NNQ::PUSH.connect(pull.last_endpoint)

        assert_equal true, !!pull.peer_connected.wait
        assert_equal true, !!push.peer_connected.wait
      ensure
        push&.close
        pull&.close
      end
    end


    it "resolves with nil when the socket is closed without any peers" do
      Sync do
        pull = NNQ::PULL.bind("tcp://127.0.0.1:0")
        pull.close
        assert_nil pull.peer_connected.wait
      end
    end
  end


  describe "#all_peers_gone" do
    it "resolves once all peers disconnect after at least one connected" do
      Sync do
        pull = NNQ::PULL.bind("tcp://127.0.0.1:0")
        push = NNQ::PUSH.connect(pull.last_endpoint)
        pull.peer_connected.wait

        push.close
        assert_equal true, pull.all_peers_gone.wait
      ensure
        pull&.close
      end
    end


    it "does not resolve if no peer ever connected" do
      Sync do
        pull = NNQ::PULL.bind("tcp://127.0.0.1:0")
        refute pull.all_peers_gone.resolved?
      ensure
        pull&.close
      end
    end
  end


  describe "#close_read" do
    it "wakes a blocked receive with nil without touching sends" do
      Sync do |task|
        pair_a = NNQ::PAIR.bind("tcp://127.0.0.1:0")
        pair_b = NNQ::PAIR.connect(pair_a.last_endpoint)
        pair_a.peer_connected.wait

        reader = task.async { pair_a.receive }
        # Give the reader a tick to block on dequeue.
        task.yield
        pair_a.close_read
        assert_nil reader.wait
      ensure
        pair_a&.close
        pair_b&.close
      end
    end
  end


  describe "#reconnect_enabled" do
    it "defaults to true and accepts writes" do
      Sync do
        pull = NNQ::PULL.bind("tcp://127.0.0.1:0")
        assert_equal true, pull.reconnect_enabled
        pull.reconnect_enabled = false
        assert_equal false, pull.reconnect_enabled
      ensure
        pull&.close
      end
    end
  end


  describe "#monitor" do
    it "emits :listening on bind and :connected on handshake" do
      Sync do
        events = []
        pull   = NNQ::PULL.new
        pull.monitor { |ev| events << ev.type }
        pull.bind("tcp://127.0.0.1:0")
        push = NNQ::PUSH.connect(pull.last_endpoint)
        pull.peer_connected.wait
        # Let monitor task drain.
        Async::Task.current.yield
        assert_includes events, :listening
        assert_includes events, :handshake_succeeded
        assert_includes events, :connected
      ensure
        push&.close
        pull&.close
      end
    end


    it "emits :disconnected when a peer goes away" do
      Sync do
        events = []
        pull   = NNQ::PULL.bind("tcp://127.0.0.1:0")
        pull.monitor { |ev| events << ev.type }
        push = NNQ::PUSH.connect(pull.last_endpoint)
        pull.peer_connected.wait
        push.close
        pull.all_peers_gone.wait
        Async::Task.current.yield
        assert_includes events, :disconnected
      ensure
        pull&.close
      end
    end
  end
end
