# frozen_string_literal: true

require "async"
require "async/limited_queue"
require_relative "backtrace"

module NNQ
  module Routing
    # Raw SURVEYOR: fans out surveys to all peers like cooked
    # {Surveyor}, but without a survey window, survey-id matching,
    # or timeout. Replies are delivered as `[pipe, header, body]`
    # tuples so the app can correlate by header verbatim.
    #
    # Each per-conn send queue holds `[header, body]` pairs and the
    # pump calls `conn.write_message(body, header: header)` so the
    # protocol-sp header kwarg is threaded through the fan-out —
    # zero concat even on the broadcast path.
    #
    class SurveyorRaw
      include Backtrace


      def initialize(engine)
        @engine     = engine
        @queues     = {} # conn => Async::LimitedQueue
        @pump_tasks = {} # conn => Async::Task
        @recv_queue = Async::LimitedQueue.new(engine.options.recv_hwm)
      end


      def send(body, header:)
        @queues.each_value do |q|
          q.enqueue([header, body]) unless q.limited?
        end
      end


      def receive
        @recv_queue.dequeue
      end


      def enqueue(wire_bytes, conn)
        header, payload = parse_backtrace(wire_bytes)
        return unless header
        @recv_queue.enqueue([conn, header, payload])
      end


      def connection_added(conn)
        queue             = Async::LimitedQueue.new(@engine.options.send_hwm)
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
        @queues.each_value.all? { |q| q.empty? }
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
        annotation = "nnq surveyor_raw pump #{conn.endpoint}"
        parent     = @engine.connections[conn]&.barrier || @engine.barrier

        @engine.spawn_task(annotation:, parent:) do
          loop do
            header, body = queue.dequeue
            conn.send_message(body, header: header)
            @engine.emit_verbose_msg_sent(body)
          rescue EOFError, IOError, Errno::EPIPE, Errno::ECONNRESET
            break
          end
        end
      end

    end
  end
end
