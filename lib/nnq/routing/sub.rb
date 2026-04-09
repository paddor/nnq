# frozen_string_literal: true

require "async/queue"

module NNQ
  module Routing
    # SUB side of the pub/sub pattern (nng sub0).
    #
    # All filtering happens locally — pub0 broadcasts blindly and sub0
    # drops messages that don't match any subscription prefix. An empty
    # subscription set means receive nothing (matching nng — unlike
    # ZeroMQ's pre-4.x "no subscription = receive everything").
    #
    # Subscriptions are byte-prefix matches. A subscription to the
    # empty string matches every message.
    #
    class Sub
      def initialize
        @queue         = Async::Queue.new
        @subscriptions = [] # array of byte strings
      end


      def subscribe(prefix)
        prefix = prefix.b
        @subscriptions << prefix unless @subscriptions.include?(prefix)
      end


      def unsubscribe(prefix)
        @subscriptions.delete(prefix.b)
      end


      def enqueue(body, _conn = nil)
        return unless matches?(body)
        @queue.enqueue(body)
      end


      # @return [String, nil]
      def receive
        @queue.dequeue
      end


      def close
        @queue.enqueue(nil)
      end

      private

      def matches?(body)
        @subscriptions.any? { |prefix| body.start_with?(prefix) }
      end
    end
  end
end
