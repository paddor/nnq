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
    # - At most one in-flight request per socket. Issuing a new
    #   send_request while a previous one is still waiting for its
    #   reply cancels the previous one: the blocked caller wakes up
    #   with a {NNQ::RequestCancelled} error and the late reply (if
    #   any) is silently dropped. This matches nng cooked req0, where
    #   a new nng_sendmsg abandons the prior request.
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
      # If another fiber issues a send_request while this call is
      # waiting, this call raises {NNQ::RequestCancelled}.
      #
      # @param body [String]
      # @return [String]
      def send_request(body)
        id      = SecureRandom.random_number(0x80000000) | 0x80000000
        promise = Async::Promise.new

        @mutex.synchronize do
          # Cancel any in-flight request — new send supersedes it.
          @outstanding&.last&.reject(RequestCancelled.new("cancelled by new send_request"))
          @outstanding = [id, promise]
        end

        conn   = pick_peer
        header = [id].pack("N")
        conn.send_message(header + body)
        promise.wait
      ensure
        @mutex.synchronize do
          # Only clear the slot if it's still ours. If a concurrent
          # send_request already replaced it, leave the new entry alone.
          @outstanding = nil if @outstanding && @outstanding[0] == id
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
