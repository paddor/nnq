# frozen_string_literal: true

require "async"
require "async/queue"
require "async/limited_queue"

module NNQ
  module Routing
    # BUS0: best-effort bidirectional mesh.
    #
    # Send side: fan-out to all connected peers. Each peer gets its own
    # bounded send queue and pump fiber — a slow peer drops messages
    # instead of blocking fast ones (same as PUB). Send never blocks.
    #
    # Recv side: all incoming messages are pushed into a shared
    # unbounded queue (same as PULL).
    #
    # No SP headers in cooked mode — body on the wire is the user
    # payload.
    #
    class Bus
      def initialize(engine)
        @engine     = engine
        @queues     = {} # conn => Async::LimitedQueue
        @pump_tasks = {} # conn => Async::Task
        @recv_queue = Async::Queue.new
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


      # Called by the engine recv loop with each received message.
      def enqueue(body, _conn = nil)
        @recv_queue.enqueue(body)
      end


      # @return [String, nil] message body, or nil once the socket is closed
      def receive
        @recv_queue.dequeue
      end


      def connection_added(conn)
        queue = Async::LimitedQueue.new(@engine.options.send_hwm)
        @queues[conn]     = queue
        @pump_tasks[conn] = spawn_pump(conn, queue)
      end


      def connection_removed(conn)
        @queues.delete(conn)
        task = @pump_tasks.delete(conn)
        return unless task
        return if task == Async::Task.current
        task.stop
      rescue IOError, Errno::EPIPE
      end


      def send_queue_drained?
        @queues.each_value.all?(&:empty?)
      end


      def close
        @pump_tasks.each_value(&:stop)
        @pump_tasks.clear
        @queues.clear
        @recv_queue.enqueue(nil)
      end


      def close_read
        @recv_queue.enqueue(nil)
      end


      private


      def spawn_pump(conn, queue)
        annotation = "nnq bus pump #{conn.endpoint}"
        barrier    = @engine.connections[conn]&.barrier || @engine.barrier

        @engine.spawn_task(annotation:, barrier:) do
          loop do
            body = queue.dequeue
            conn.send_message(body)
            @engine.emit_verbose_monitor_event(:message_sent, body: body)
          rescue EOFError, IOError, Errno::EPIPE, Errno::ECONNRESET
            break
          end
        end
      end

    end
  end
end
