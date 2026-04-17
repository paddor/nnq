# frozen_string_literal: true

module NNQ
  module Transport
    module Inproc
      # Queue-based in-process pipe. Duck-types {NNQ::Connection} so
      # routing strategies, the recv loop, and the send pump work
      # against it unchanged.
      #
      # No wire framing: bodies are transferred as frozen Strings
      # through a pair of {Async::Queue} (one per direction). When an
      # SP backtrace header is supplied (REQ/REP/SURVEYOR paths), it's
      # prepended before enqueue so {#receive_message} returns an
      # already-prefixed body — matching the TCP/IPC framing semantic
      # so routing's `parse_backtrace` parses the same layout either
      # way.
      #
      # Direct-recv fast path: when a routing strategy calls
      # {#wire_direct_recv} on the peer side of a pipe pair, subsequent
      # {#send_message} calls enqueue straight into the consumer's
      # recv queue — the intermediate pipe queue and the recv pump
      # fiber are both skipped. Cuts three fiber hops to one and is
      # what lets inproc PUSH/PULL clear 1M msg/s on YJIT.
      #
      # Wiring happens synchronously inside {Transport::Inproc.connect}
      # (before the call returns to the caller), so there's no window
      # in which a send can precede a wire — no pending buffer needed.
      #
      # Close protocol: {#close} enqueues a `nil` sentinel onto the
      # send side (or the direct queue if wired). The peer's recv loop
      # sees `nil`, raises `EOFError`, and unwinds via its connection
      # supervisor.
      class Pipe
        # @return [String, nil] endpoint URI this pipe was established on
        attr_reader :endpoint

        # @return [Pipe, nil] the other end of the pair
        attr_accessor :peer

        # @return [Async::Queue, nil] when non-nil, {#send_message}
        #   enqueues here instead of into @send_queue.
        attr_reader :direct_recv_queue


        def initialize(send_queue:, recv_queue:, endpoint:)
          @send_queue            = send_queue
          @recv_queue            = recv_queue
          @endpoint              = endpoint
          @closed                = false
          @peer                  = nil
          @direct_recv_queue     = nil
          @direct_recv_transform = nil
        end


        # Wires the direct-recv fast path. After this call, messages
        # sent on this pipe bypass the intermediate pipe queue and
        # land directly in +queue+.
        #
        # @param queue [Async::Queue]
        # @param transform [Proc, nil] optional per-message transform;
        #   return nil to drop the message (used by filter/parse
        #   strategies like SUB or REP).
        def wire_direct_recv(queue, transform)
          @direct_recv_transform = transform
          @direct_recv_queue     = queue
        end


        def send_message(body, header: nil)
          raise ClosedError, "connection closed" if @closed
          wire = header ? header + body : body

          if (q = @direct_recv_queue)
            item = @direct_recv_transform ? @direct_recv_transform.call(wire) : wire
            q.enqueue(item) unless item.nil?
          else
            @send_queue.enqueue(wire)
          end
        end


        alias write_message send_message


        def write_messages(bodies)
          raise ClosedError, "connection closed" if @closed

          if (q = @direct_recv_queue)
            transform = @direct_recv_transform
            bodies.each do |body|
              item = transform ? transform.call(body) : body
              q.enqueue(item) unless item.nil?
            end
          else
            bodies.each { |body| @send_queue.enqueue(body) }
          end
        end


        # No-op — Async::Queue has no IO buffer to flush.
        def flush
          nil
        end


        def receive_message
          item = @recv_queue.dequeue
          raise EOFError, "connection closed" if item.nil?
          item
        end


        def closed?
          @closed
        end


        def close
          return if @closed
          @closed = true
          # Close sentinel goes on whichever queue the peer is reading.
          # When direct-wired, @send_queue is unused; hit the direct
          # queue so the consumer unblocks.
          (@direct_recv_queue || @send_queue).enqueue(nil)
        end

      end
    end
  end
end
