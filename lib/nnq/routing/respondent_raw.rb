# frozen_string_literal: true

require "async/limited_queue"
require_relative "backtrace"

module NNQ
  module Routing
    # Raw RESPONDENT: mirror of {RepRaw} for the survey pattern.
    # No survey-window state, no pending slot — the app receives
    # `[pipe, header, body]` tuples and chooses whether (and when)
    # to reply via `send(body, to:, header:)`.
    #
    class RespondentRaw
      include Backtrace


      def initialize(engine)
        @engine     = engine
        @recv_queue = Async::LimitedQueue.new(engine.options.recv_hwm)
      end


      def receive
        @recv_queue.dequeue
      end


      def send(body, to:, header:)
        return if to.closed?
        return if Backtrace.too_many_hops?(header)
        to.send_message(body, header: header)
        @engine.emit_verbose_msg_sent(body)
      rescue ClosedError
      end


      def preview_body(wire)
        _, payload = parse_backtrace(wire)
        payload || wire
      end


      def enqueue(wire_bytes, conn)
        header, payload = parse_backtrace(wire_bytes)
        return unless header
        @recv_queue.enqueue([conn, header, payload])
      end


      # Inproc fast-path hook.
      def direct_recv_for(conn)
        transform = lambda do |wire_bytes|
          header, payload = parse_backtrace(wire_bytes)
          header ? [conn, header, payload] : nil
        end
        [@recv_queue, transform]
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
