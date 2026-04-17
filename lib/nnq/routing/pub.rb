# frozen_string_literal: true

require "async"
require "async/limited_queue"

module NNQ
  module Routing
    # PUB side of the pub/sub pattern (nng pub0).
    #
    # Broadcasts every message to every connected SUB. Each peer gets
    # its own bounded send queue (`send_hwm`) and its own send pump
    # fiber — a slow subscriber cannot block fast ones. When a peer's
    # queue is full, new messages are dropped for that peer (matching
    # nng's non-blocking fan-out semantics).
    #
    # Pub0 has no subscription state on the sender side: SUBs filter
    # locally. Pub0 is strictly one-directional; nothing is read from
    # SUB peers.
    #
    class Pub
      def initialize(engine)
        @engine = engine
        @queues = {} # conn => Async::LimitedQueue
      end


      # Broadcasts +body+ to every connected peer. Non-blocking per
      # peer: drops when a peer's queue is at HWM.
      #
      # @param body [String]
      def send(body)
        @queues.each_value do |queue|
          queue.enqueue(body) unless queue.limited?
        end
      end


      def connection_added(conn)
        queue = Async::LimitedQueue.new(@engine.options.send_hwm)
        # Register queue BEFORE spawning the pump. spawn_task yields
        # control into the new task body, which parks on queue.dequeue;
        # at that park the publisher fiber can run and must already see
        # this peer's queue.
        @queues[conn] = queue
        spawn_pump(conn, queue)
      end


      def connection_removed(conn)
        @queues.delete(conn)
      end


      # True once every peer's queue is empty. Engine linger polls this.
      def send_queue_drained?
        @queues.each_value.all? { |q| q.empty? }
      end


      def close
        @queues.clear
      end


      private


      def spawn_pump(conn, queue)
        annotation = "nnq pub pump #{conn.endpoint}"
        parent     = @engine.connections[conn]&.barrier || @engine.barrier

        @engine.spawn_task(annotation:, parent:) do
          loop do
            body = queue.dequeue
            conn.send_message(body)
            @engine.emit_verbose_msg_sent(body)
          rescue EOFError, IOError, Errno::EPIPE, Errno::ECONNRESET
            break
          end
        end
      end

    end
  end
end
