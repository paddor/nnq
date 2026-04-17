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
      # Close protocol: {#close} enqueues a `nil` sentinel onto the
      # send side. The peer's recv loop sees `nil`, raises `EOFError`,
      # and unwinds via its connection supervisor.
      class Pipe
        # @return [String, nil] endpoint URI this pipe was established on
        attr_reader :endpoint

        # @return [Pipe, nil] the other end of the pair
        attr_accessor :peer


        def initialize(send_queue:, recv_queue:, endpoint:)
          @send_queue = send_queue
          @recv_queue = recv_queue
          @endpoint   = endpoint
          @closed     = false
          @peer       = nil
        end


        def send_message(body, header: nil)
          raise ClosedError, "connection closed" if @closed
          @send_queue.enqueue(header ? header + body : body)
        end


        alias write_message send_message


        def write_messages(bodies)
          raise ClosedError, "connection closed" if @closed
          bodies.each { |body| @send_queue.enqueue(body) }
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
          @send_queue.enqueue(nil)
        end

      end
    end
  end
end
