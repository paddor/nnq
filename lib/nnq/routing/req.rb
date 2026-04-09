# frozen_string_literal: true

require "async"
require "securerandom"

module NNQ
  module Routing
    # REQ: client side of req0/rep0.
    #
    # Wire format: each message body on the wire is `[4-byte BE
    # request_id][user_payload]`. The request id has the high bit set
    # (`0x80000000..0xFFFFFFFF`) — that's nng's marker for "this is the
    # last (deepest) frame on the backtrace stack". Direct REQ→REP has
    # exactly one id.
    #
    # Semantics (cooked mode, what this implements):
    # - Single in-flight request per socket. The user fiber sends, the
    #   call blocks until the matching reply comes back, then unblocks.
    #   Concurrent calls raise (matches nng cooked req0 — use nng_ctx
    #   for parallelism, which we don't model here).
    # - Reply is matched by id, NOT by pipe. Late or unmatched replies
    #   are silently dropped.
    # - Round-robin peer selection, but no retry timer (real nng resends
    #   on a timer; we leave that to the user via timeouts).
    # - Blocks waiting for a peer if no connection is currently up.
    #
    class Req
      def initialize(engine)
        @engine     = engine
        @next_idx   = 0
        @mutex      = Mutex.new
        @outstanding = nil # [id, promise] or nil
      end


      # Sends +body+ as a request, blocks until the matching reply
      # comes back. Returns the reply payload (without the id header).
      #
      # @param body [String]
      # @return [String]
      def send_request(body)
        id      = SecureRandom.random_number(0x80000000) | 0x80000000
        promise = Async::Promise.new

        @mutex.synchronize do
          raise Error, "REQ socket already has a request in flight" if @outstanding
          @outstanding = [id, promise]
        end

        begin
          conn   = pick_peer
          header = [id].pack("N")
          conn.send_message(header + body)
          promise.wait
        ensure
          @mutex.synchronize { @outstanding = nil }
        end
      end


      # Called by the engine recv loop with each received frame.
      def enqueue(body, _conn)
        return if body.bytesize < 4
        id      = body.unpack1("N")
        payload = body.byteslice(4..)

        @mutex.synchronize do
          if @outstanding && @outstanding[0] == id
            @outstanding[1].resolve(payload)
          end
          # Mismatched id → late/spurious reply, silently dropped.
        end
      end


      def close
        @mutex.synchronize do
          @outstanding&.last&.reject(NNQ::Error.new("REQ socket closed"))
          @outstanding = nil
        end
      end


      private

      def pick_peer
        loop do
          conns = @engine.connections
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
