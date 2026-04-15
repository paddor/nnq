# frozen_string_literal: true

require "async/queue"
require_relative "backtrace"

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
    # - At most one pending request at a time. Calling #receive while a
    #   previous request is pending silently discards that request — its
    #   backtrace is forgotten and any later #send_reply will target the
    #   *new* request. This matches nng cooked rep0, where nng_recvmsg
    #   after nng_recvmsg drops the earlier message.
    # - Calling #send_reply with no pending request raises.
    # - The reply must be routed back to the same pipe the request came
    #   from. If that pipe died in the meantime, #send_reply silently
    #   drops the reply (matches nng's pipe_terminated behavior).
    # - TTL cap on the backtrace stack: 8 hops, matching nng's default.
    #
    class Rep
      include Backtrace


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
        # Any prior pending request is discarded — calling receive
        # again without replying is how users drop unwanted requests.
        @mutex.synchronize { @pending = nil }
        item = @recv_queue.dequeue

        return nil if item.nil?

        conn, btrace, body = item
        @mutex.synchronize { @pending = [conn, btrace] }
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


      # Called by the engine recv loop with each received message.
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


      def close_read
        @recv_queue.enqueue(nil)
      end


    end
  end
end
