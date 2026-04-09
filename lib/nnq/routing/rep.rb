# frozen_string_literal: true

require "async/queue"

module NNQ
  module Routing
    # REP: server side of req0/rep0.
    #
    # Wire format: incoming bodies are `[backtrace stack][user_payload]`.
    # The backtrace is one or more 4-byte BE words; we keep reading words
    # off the front until we hit one whose top byte has its high bit set
    # (the original REQ's request id terminates the stack). The whole
    # backtrace is stashed and echoed verbatim on reply, prepended to the
    # reply body. REP never reorders or rewrites the stack — it's pure
    # echo back to the originating pipe.
    #
    # Semantics (cooked mode):
    # - Strict alternation: receive → send → receive. Calling #send_reply
    #   without a pending request raises; calling #receive while one is
    #   already pending also raises (use nng_ctx for parallelism — not
    #   modeled here).
    # - The reply must be routed back to the same pipe the request came
    #   from. If that pipe died in the meantime, #send_reply silently
    #   drops the reply (matches nng's pipe_terminated behavior).
    # - TTL cap on the backtrace stack: 8 hops, matching nng's default.
    #
    class Rep
      MAX_HOPS = 8 # nng's default ttl

      def initialize(engine)
        @engine     = engine
        @recv_queue = Async::Queue.new   # holds [conn, btrace, body]
        @pending    = nil                # [conn, btrace] or nil
        @mutex      = Mutex.new
      end


      # Receives one request body. Stashes the backtrace + originating
      # connection so the next #send_reply can route the reply back.
      #
      # @return [String, nil] body, or nil if the socket was closed
      def receive
        item = @recv_queue.dequeue
        return nil if item.nil?
        conn, btrace, body = item
        @mutex.synchronize do
          raise Error, "REP socket already has a pending request" if @pending
          @pending = [conn, btrace]
        end
        body
      end


      # Sends +body+ as the reply to the most recently received request.
      #
      # @param body [String]
      def send_reply(body)
        conn, btrace = @mutex.synchronize do
          raise Error, "REP socket has no pending request to reply to" unless @pending
          taken    = @pending
          @pending = nil
          taken
        end

        return if conn.closed?
        conn.send_message(btrace + body)
      end


      # Called by the engine recv loop with each received frame.
      def enqueue(body, conn)
        btrace, payload = parse_backtrace(body)
        return unless btrace # malformed/over-TTL — drop
        @recv_queue.enqueue([conn, btrace, payload])
      end


      def connection_removed(conn)
        @mutex.synchronize do
          @pending = nil if @pending && @pending[0] == conn
        end
      end


      def close
        @recv_queue.enqueue(nil)
      end


      private

      # Reads 4-byte BE words off the front of +body+, stopping at the
      # first one whose top byte has its high bit set. Returns
      # [backtrace_bytes, remaining_payload] or nil on malformed input.
      def parse_backtrace(body)
        offset = 0
        hops   = 0
        while hops < MAX_HOPS
          return nil if body.bytesize - offset < 4
          word = body.byteslice(offset, 4)
          offset += 4
          hops   += 1
          if word.getbyte(0) & 0x80 != 0
            return [body.byteslice(0, offset), body.byteslice(offset..)]
          end
        end
        nil # exceeded TTL without finding terminator
      end
    end
  end
end
