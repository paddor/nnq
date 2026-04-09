# frozen_string_literal: true

require "async/queue"

module NNQ
  module Routing
    # PULL side: an unbounded queue of received messages. Per-connection
    # recv fibers (spawned by the Engine when each pipe is established)
    # call {#enqueue} on each frame; user code calls {#receive}.
    #
    # No HWM, no prefetch buffer — TCP throttles the senders directly
    # via the kernel buffer.
    #
    class Pull
      def initialize
        @queue = Async::Queue.new
      end


      def enqueue(body, _conn = nil)
        @queue.enqueue(body)
      end


      # @return [String, nil] message body, or nil if the queue was closed
      def receive
        @queue.dequeue
      end


      # Wakes any waiters with nil so receive returns from a closed
      # socket.
      def close
        @queue.enqueue(nil)
      end


      # Wakes any waiters with nil, leaving the send side untouched
      # (PULL has no send side — close_read is identical to close here,
      # but kept separate for the `Socket#close_read` contract).
      def close_read
        @queue.enqueue(nil)
      end
    end
  end
end
