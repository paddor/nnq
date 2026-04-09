# frozen_string_literal: true

require "async/queue"
require_relative "send_pump"

module NNQ
  module Routing
    # PAIR0: exclusive bidirectional channel with a single peer.
    #
    # Wire format: no SP header. Body on the wire is exactly the user
    # payload (same as push0/pull0). Per nng's pair0, when a second peer
    # tries to connect while one is already paired, the new pipe is
    # rejected — first peer wins.
    #
    # Send side: shared send queue + 1 pump (reuses {SendPump}). The
    # pump infrastructure is identical to PUSH; PAIR just never has
    # more than one pump because it never has more than one peer.
    #
    # Recv side: messages fed by the engine's recv loop into a local
    # Async::Queue. Unbounded — TCP throttles the peer.
    #
    class Pair
      include SendPump

      def initialize(engine)
        init_send_pump(engine)
        @recv_queue = Async::Queue.new
        @peer       = nil
      end


      # @param body [String]
      def send(body)
        enqueue_for_send(body)
      end


      # @return [String, nil] message body, or nil once the socket is closed
      def receive
        @recv_queue.dequeue
      end


      # Called by the recv loop with each frame off the wire.
      def enqueue(body, _conn = nil)
        @recv_queue.enqueue(body)
      end


      # First-pipe-wins. If we already have a peer, signal the engine
      # to drop the new connection.
      def connection_added(conn)
        if @peer
          conn.close
          @engine.handle_connection_lost(conn)
          return
        end
        @peer = conn
        spawn_send_pump_for(conn)
      end


      def connection_removed(conn)
        remove_send_pump_for(conn)
        @peer = nil if @peer == conn
      end


      def close
        super
        @recv_queue.enqueue(nil) # wake any waiter
      end
    end
  end
end
