# frozen_string_literal: true

require "async/limited_queue"
require_relative "backtrace"

module NNQ
  module Routing
    # Raw REP: bypasses the cooked state machine. The incoming
    # backtrace header is split off once at parse time and handed to
    # the caller alongside the live Connection as `[pipe, header, body]`.
    # Replies go back via `send(body, to:, header:)` which writes the
    # caller-supplied header verbatim — no cooked pending/echo logic,
    # no single-in-flight constraint.
    #
    class RepRaw
      include Backtrace


      def initialize(engine)
        @engine     = engine
        @recv_queue = Async::LimitedQueue.new(engine.options.recv_hwm)
      end


      # @return [Array, nil] [pipe, header, body] or nil on close
      def receive
        @recv_queue.dequeue
      end


      # Sends +body+ with the caller-supplied +header+ back to +to+
      # (a Connection handed out by a prior #receive). Silent drop
      # if the target is closed or the header would push total hops
      # over MAX_HOPS.
      def send(body, to:, header:)
        return if to.closed?
        return if Backtrace.too_many_hops?(header)
        to.send_message(body, header: header)
      rescue ClosedError
        # peer went away between receive and send — drop
      end


      # Called by the engine recv loop.
      def enqueue(wire_bytes, conn)
        header, payload = parse_backtrace(wire_bytes)
        return unless header # malformed / over-TTL — drop
        @recv_queue.enqueue([conn, header, payload])
      end


      def close
        @recv_queue.enqueue(nil)
      end


      def close_read
        @recv_queue.enqueue(nil)
      end

    end
  end
end
