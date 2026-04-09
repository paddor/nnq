# frozen_string_literal: true

require_relative "send_pump"

module NNQ
  module Routing
    # PUSH side of the pipeline pattern.
    #
    # Architecture: ONE shared bounded send queue per socket. Each peer
    # connection gets its own send pump fiber that races to dequeue from
    # the shared queue and write to its peer (work-stealing). A slow
    # peer's pump just stops pulling (blocked on its own TCP flush);
    # fast peers' pumps keep draining. Strictly better than per-pipe
    # round-robin for PUSH semantics — load naturally biases to whoever
    # is keeping up.
    #
    class Push
      include SendPump

      def initialize(engine)
        init_send_pump(engine)
      end


      # User-facing send: enqueue onto the shared send queue.
      #
      # @param body [String]
      def send(body)
        enqueue_for_send(body)
      end


      def connection_added(conn)
        spawn_send_pump_for(conn)
      end


      def connection_removed(conn)
        remove_send_pump_for(conn)
      end
    end
  end
end
