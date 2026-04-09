# frozen_string_literal: true

require_relative "../../test_helper"
require "nnq/send/staging"

module NNQ
  module Send
    class StagingTest < Minitest::Test
      class FakeConn
        attr_reader :flush_batches

        def initialize
          @buffer        = []
          @flush_batches = []
        end

        def write_message(bytes)
          @buffer << bytes
        end

        def flush
          @flush_batches << @buffer.dup
          @buffer.clear
        end
      end


      def test_single_send_round_trip
        conn = FakeConn.new
        Sync { Send::Staging.new(conn).commit("hello") }
        assert_equal [["hello"]], conn.flush_batches
      end


      def test_sequential_sends_each_get_their_own_flush
        conn = FakeConn.new
        Sync do
          staging = Send::Staging.new(conn)
          staging.commit("a")
          staging.commit("b")
          staging.commit("c")
        end
        assert_equal [["a"], ["b"], ["c"]], conn.flush_batches
      end


      def test_concurrent_senders_all_delivered
        # Tests the *correctness* contract: under N concurrent fibers
        # calling commit, every message is delivered exactly once and
        # none are dropped. The exact batching pattern is opportunistic
        # and not part of the contract — covered by benchmarks instead.
        conn = FakeConn.new
        Sync do |task|
          staging = Send::Staging.new(conn)
          barrier = Async::Barrier.new
          50.times { |i| barrier.async(parent: task) { staging.commit("m#{i}") } }
          barrier.wait
        end

        delivered = conn.flush_batches.flatten
        assert_equal 50, delivered.size
        assert_equal (0...50).map { |i| "m#{i}" }.sort, delivered.sort
      end


      # FakeConn whose flush yields once to the scheduler — simulates a
      # real connection where flush parks on wait_writable, giving other
      # senders a chance to enqueue.
      class YieldingFakeConn < FakeConn
        def flush
          # Cooperatively yield to the scheduler so other senders waiting
          # in the run queue get a chance to enqueue into @pending while
          # the drainer is "in flight". Stand-in for parking on
          # `wait_writable` against a real socket.
          Async::Task.current.yield
          super
        end
      end


      def test_concurrent_senders_coalesce_at_least_once
        # When the drainer yields inside flush (the real-world case
        # where it's parked on wait_writable), concurrent senders pile
        # up in @pending and the drainer batches them on its next
        # iteration. We don't assert exact batch sizes — just that
        # batching is happening at all (fewer flushes than messages).
        #
        # Note: `task.async { ... }` runs the child eagerly until its
        # first yield. To force concurrent enqueueing we park all 50
        # senders on a shared Condition first, then broadcast.
        conn  = YieldingFakeConn.new
        start = Async::Condition.new
        Sync do |task|
          staging = Send::Staging.new(conn)
          tasks = 50.times.map do |i|
            task.async do
              start.wait
              staging.commit("m#{i}")
            end
          end
          start.signal # broadcast: all 50 become ready simultaneously
          tasks.each(&:wait)
        end

        # All 50 messages must still be delivered.
        assert_equal 50, conn.flush_batches.flatten.size
        # And batching must have actually happened.
        assert_operator conn.flush_batches.size, :<, 50,
          "expected coalescing, got one flush per message (batches: #{conn.flush_batches.map(&:size).inspect})"
      end


      def test_flush_failure_propagates_and_clears_drainer
        conn = Object.new
        def conn.write_message(_); end
        def conn.flush; raise IOError, "boom"; end

        Sync do
          staging = Send::Staging.new(conn)
          assert_raises(IOError) { staging.commit("x") }

          # Drainer flag must be cleared so subsequent commits can drain.
          def conn.flush; end # heal
          staging.commit("y")  # must not deadlock
        end
      end
    end
  end
end
