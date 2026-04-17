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


      # Called by the recv loop with each message off the wire.
      def enqueue(body, _conn = nil)
        @recv_queue.enqueue(body)
      end


      # Inproc fast-path hook: peer pipe enqueues straight into the
      # local recv queue.
      def direct_recv_for(_conn)
        [@recv_queue, nil]
      end


      # First-pipe-wins. Raising {ConnectionRejected} tells the
      # ConnectionLifecycle to tear down the just-registered connection
      # without ever exposing it to pumps.
      def connection_added(conn)
        raise ConnectionRejected, "PAIR socket already has a peer" if @peer

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


      # Wake recv side without tearing down the send pump.
      def close_read
        @recv_queue.enqueue(nil)
      end

    end
  end
end
