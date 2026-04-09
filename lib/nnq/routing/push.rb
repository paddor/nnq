# frozen_string_literal: true

require "async"

module NNQ
  module Routing
    # PUSH side: round-robin send across all live peer connections, block
    # the caller while there is no live connection.
    #
    class Push
      def initialize(connections, condition)
        @connections = connections
        @condition   = condition
        @cycle       = connections.cycle
      end


      # Picks the next live connection (round-robin) and stages +body+ on
      # it. Blocks until at least one live connection exists.
      #
      # @param body [String]
      # @return [void]
      def send(body)
        loop do
          conn = pick_live
          return conn.send_message(body) if conn
          @condition.wait
        end
      end


      private

      def pick_live
        return nil if @connections.empty?
        @connections.size.times do
          conn = @cycle.next
          return conn unless conn.closed?
        end
        nil
      end
    end
  end
end
