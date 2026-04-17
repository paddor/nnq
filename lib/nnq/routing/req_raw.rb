# frozen_string_literal: true

require "async/limited_queue"
require_relative "backtrace"

module NNQ
  module Routing
    # Raw REQ: bypasses the cooked single-in-flight request-id
    # state machine. Sends are fire-and-forget round-robin with a
    # caller-supplied header (typically `[id | 0x80000000].pack("N")`);
    # replies land in a bounded queue and are delivered as
    # `[pipe, header, body]` tuples so the app can correlate by
    # header verbatim without ever parsing or slicing bytes.
    #
    class ReqRaw
      include Backtrace


      def initialize(engine)
        @engine     = engine
        @next_idx   = 0
        @recv_queue = Async::LimitedQueue.new(engine.options.recv_hwm)
      end


      def send(body, header:)
        conn = pick_peer
        conn.send_message(body, header: header)
        @engine.emit_verbose_msg_sent(body)
      end


      def preview_body(wire)
        _, payload = parse_backtrace(wire)
        payload || wire
      end


      def receive
        @recv_queue.dequeue
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


      private


      def pick_peer
        loop do
          conns = @engine.connections.keys

          if conns.empty?
            @engine.new_pipe.wait
            next
          end

          @next_idx = (@next_idx + 1) % conns.size
          return conns[@next_idx]
        end
      end

    end
  end
end
