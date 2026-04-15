# frozen_string_literal: true

require "async"
require "async/limited_queue"

module NNQ
  module Routing
    # Mixin for routing strategies that drain a shared bounded send queue
    # via per-connection work-stealing pumps. Used by PUSH (load-balance
    # across N peers) and PAIR (single peer, but the same pump shape).
    #
    # See DESIGN.md "Per-socket HWM" for the rationale.
    #
    # The per-pump batch caps (BATCH_MSG_CAP / BATCH_BYTE_CAP) enforce
    # fairness across the work-stealing pumps. Without them, the first
    # pump that wakes up would drain the entire queue in one non-blocking
    # burst before any other pump got a turn (TCP send buffers absorb
    # bursts without forcing a fiber yield).
    #
    # Including classes must call {#init_send_pump} from #initialize and
    # {#spawn_send_pump_for} from their #connection_added hook.
    #
    module SendPump
      # TODO: API doc
      BATCH_MSG_CAP  = 256


      # TODO: API doc
      BATCH_BYTE_CAP = 256 * 1024


      # @return [Boolean] true once the shared queue is empty AND no
      #   batch is mid-write across any pump.
      def send_queue_drained?
        @send_queue.empty? && @in_flight.zero?
      end


      # Removes a pump and stops its task (unless called from inside
      # the pump itself, in which case the pump is already on its way
      # out via the rescue/ensure path).
      def remove_send_pump_for(conn)
        task = @pumps.delete(conn)
        return if task.nil? || task == Async::Task.current
        task.stop
      rescue IOError, Errno::EPIPE
        # Pump was mid-flush when its conn was closed; cancel surfaced
        # the same IOError. Already handled — pump is gone.
      end


      # Stops all send pump tasks. Each pump's ensure block calls
      # engine.handle_connection_lost → routing.connection_removed
      # which removes its own entry, so iterate over a snapshot.
      def close
        @pumps.values.each(&:stop)
        @pumps.clear
      end


      private


      # @param engine [Engine]
      def init_send_pump(engine)
        @engine     = engine
        @send_queue = Async::LimitedQueue.new(engine.options.send_hwm)
        @pumps      = {} # conn => pump task
        @in_flight  = 0  # batches dequeued but not yet flushed
      end


      # Enqueues +body+ on the shared send queue. Blocks the caller when
      # the queue is full (HWM backpressure).
      #
      # @param body [String]
      def enqueue_for_send(body)
        @send_queue.enqueue(body)
      end


      # Spawns a send pump fiber for +conn+ that races to drain the
      # shared queue.
      #
      # @param conn [Connection]
      def spawn_send_pump_for(conn)
        annotation = "nnq send pump #{conn.endpoint}"
        barrier    = @engine.connections[conn]&.barrier || @engine.barrier

        task = @engine.spawn_task(annotation:, barrier:) do
          loop do
            first = @send_queue.dequeue
            break if first.nil? # queue closed

            @in_flight += 1

            begin
              batch = [first]
              drain_capped(batch)
              write_batch(conn, batch)
            ensure
              @in_flight -= 1
            end

            Async::Task.current.yield
          rescue EOFError, IOError, Errno::EPIPE, Errno::ECONNRESET
            # Peer died mid-flush. In-flight batch dropped.
            break
          end
        end

        @pumps[conn] = task
      end


      def drain_capped(batch)
        bytes = batch.first.bytesize

        while batch.size < BATCH_MSG_CAP && bytes < BATCH_BYTE_CAP
          msg = @send_queue.dequeue(timeout: 0)

          break unless msg

          batch << msg
          bytes += msg.bytesize
        end
      end


      def write_batch(conn, batch)
        if batch.size == 1
          conn.write_message(batch.first)
        else
          # Single mutex acquisition for the whole batch (batches run
          # up to BATCH_MSG_CAP messages). The per-message pump loop
          # would otherwise lock/unlock the SP mutex N times.
          conn.write_messages(batch)
        end

        conn.flush

        batch.each do |body|
          @engine.emit_verbose_monitor_event(:message_sent, body: body)
        end
      end

    end
  end
end
