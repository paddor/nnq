# frozen_string_literal: true

require_relative "../test_helper"

describe NNQ::BUS0 do
  it "round-trips between two bus nodes" do
    Sync do
      a = NNQ::BUS0.bind("tcp://127.0.0.1:0")
      b = NNQ::BUS0.connect(a.last_endpoint)

      # Wait for connection to establish.
      a.peer_connected.wait

      a.send("from-a")
      b.send("from-b")

      assert_equal "from-a", b.receive
      assert_equal "from-b", a.receive
    ensure
      a&.close
      b&.close
    end
  end


  it "fans out to multiple peers" do
    Sync do
      hub = NNQ::BUS0.bind("tcp://127.0.0.1:0")
      n1  = NNQ::BUS0.connect(hub.last_endpoint)
      n2  = NNQ::BUS0.connect(hub.last_endpoint)

      # Wait for both connections.
      50.times do
        break if hub.connection_count >= 2
        sleep 0.01
      end

      hub.send("broadcast")

      Async::Task.current.with_timeout(2) do
        assert_equal "broadcast", n1.receive
        assert_equal "broadcast", n2.receive
      end
    ensure
      n1&.close
      n2&.close
      hub&.close
    end
  end


  it "handles bidirectional burst" do
    n = 100
    Sync do |task|
      a = NNQ::BUS0.bind("tcp://127.0.0.1:0")
      b = NNQ::BUS0.connect(a.last_endpoint)

      a.peer_connected.wait

      recv_b = task.async { n.times.map { b.receive } }
      recv_a = task.async { n.times.map { a.receive } }

      n.times { |i| a.send("a#{i}") }
      n.times { |i| b.send("b#{i}") }

      assert_equal n.times.map { |i| "a#{i}" }, recv_b.wait
      assert_equal n.times.map { |i| "b#{i}" }, recv_a.wait
    ensure
      a&.close
      b&.close
    end
  end
end
