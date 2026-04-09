# frozen_string_literal: true

require_relative "../test_helper"

module NNQ
  class PairTest < Minitest::Test
    def test_round_trip_over_tcp
      Sync do
        a = NNQ::PAIR.bind("tcp://127.0.0.1:0")
        b = NNQ::PAIR.connect(a.last_endpoint)

        a.send("alpha")
        b.send("beta")

        assert_equal "alpha", b.receive
        assert_equal "beta", a.receive
      ensure
        a&.close
        b&.close
      end
    end


    def test_bidirectional_burst
      n = 200
      Sync do |task|
        a = NNQ::PAIR.bind("tcp://127.0.0.1:0")
        b = NNQ::PAIR.connect(a.last_endpoint)

        recvb = task.async { n.times.map { b.receive } }
        recva = task.async { n.times.map { a.receive } }

        n.times { |i| a.send("a#{i}") }
        n.times { |i| b.send("b#{i}") }

        assert_equal n.times.map { |i| "a#{i}" }, recvb.wait
        assert_equal n.times.map { |i| "b#{i}" }, recva.wait
      ensure
        a&.close
        b&.close
      end
    end


    def test_first_pipe_wins
      Sync do
        listener = NNQ::PAIR.bind("tcp://127.0.0.1:0")
        first    = NNQ::PAIR.connect(listener.last_endpoint)

        # Give the first connection time to fully establish.
        listener.send("hello-first")
        assert_equal "hello-first", first.receive

        # A second connect should be dropped by the listener.
        second = NNQ::PAIR.connect(listener.last_endpoint)

        # First peer is still the active one.
        listener.send("still-first")
        assert_equal "still-first", first.receive
      ensure
        first&.close
        second&.close
        listener&.close
      end
    end
  end
end
