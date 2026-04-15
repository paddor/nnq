# frozen_string_literal: true

require "async"
require "async/queue"
require "async/limited_queue"
require "securerandom"

module NNQ
  module Routing
    # SURVEYOR: broadcast side of the survey0 pattern.
    #
    # Wire format: each survey is prepended with a 4-byte BE survey ID
    # (high bit set — same terminal-marker convention as REQ). Replies
    # carry the same ID back. Stale replies (wrong ID) are dropped.
    #
    # Send side: fan-out to all connected respondents (like PUB). Each
    # peer gets its own bounded queue and pump.
    #
    # Recv side: replies are matched by survey ID. Only replies
    # matching the current survey are delivered. After `survey_time`
    # elapses, {#receive} raises {NNQ::TimedOut}.
    #
    class Surveyor
      def initialize(engine)
        @engine     = engine
        @queues     = {} # conn => Async::LimitedQueue
        @pump_tasks = {} # conn => Async::Task
        @recv_queue = Async::Queue.new
        @current_id = nil
        @mutex      = Mutex.new
      end


      # Broadcasts +body+ as a survey to all connected respondents.
      # Starts a new survey window; any previous survey is abandoned.
      #
      # @param body [String]
      def send_survey(body)
        id = SecureRandom.random_number(0x80000000) | 0x80000000

        @mutex.synchronize do
          @current_id = id
        end

        header = [id].pack("N")
        wire   = header + body

        @queues.each_value do |q|
          q.enqueue(wire) unless q.limited?
        end
      end


      # Receives the next reply within the survey window. Raises
      # {NNQ::TimedOut} when the window expires.
      #
      # @return [String] reply body
      def receive
        survey_time = @engine.options.survey_time
        Fiber.scheduler.with_timeout(survey_time) { @recv_queue.dequeue }
      rescue Async::TimeoutError
        raise NNQ::TimedOut, "survey timed out"
      end


      # Called by the engine recv loop with each received message.
      def enqueue(body, _conn)
        return if body.bytesize < 4

        id      = body.unpack1("N")
        payload = body.byteslice(4..)

        @mutex.synchronize do
          return unless @current_id == id
        end

        @recv_queue.enqueue(payload)
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
        annotation = "nnq surveyor pump #{conn.endpoint}"
        parent     = @engine.connections[conn]&.barrier || @engine.barrier

        @engine.spawn_task(annotation:, parent:) do
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
