# frozen_string_literal: true

require_relative "../test_helper"

module NNQ
  class PushPullTest < Minitest::Test
    def test_round_trip_over_tcp
      Sync do
        pull = NNQ::PULL.bind("tcp://127.0.0.1:0")
        endpoint = pull.last_endpoint
        push = NNQ::PUSH.connect(endpoint)

        push.send("hello")
        push.send("world")

        assert_equal "hello", pull.receive
        assert_equal "world", pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    def test_many_messages_over_tcp
      n = 1_000
      Sync do
        pull = NNQ::PULL.bind("tcp://127.0.0.1:0")
        push = NNQ::PUSH.connect(pull.last_endpoint)

        sender = Async::Task.current.async do
          n.times { |i| push.send("m#{i}") }
        end

        n.times { |i| assert_equal "m#{i}", pull.receive }
        sender.wait
      ensure
        push&.close
        pull&.close
      end
    end
  end
end
