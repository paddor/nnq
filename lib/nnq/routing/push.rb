# frozen_string_literal: true

require "async"
require "async/limited_queue"

module NNQ
  module Routing
    # PUSH side of the pipeline pattern.
    #
    # Architecture: ONE shared bounded send queue per socket. Each peer
    # connection gets its own send pump fiber that races to dequeue from
    # the shared queue and write to its peer (work-stealing). A slow peer's
    # pump just stops pulling (blocked on its own TCP flush); fast peers'
    # pumps keep draining. This is strictly better than per-pipe round-robin
    # for PUSH semantics — load naturally biases to whoever is keeping up.
    #
    # `send_hwm` bounds the *one* shared queue, not the per-peer queues.
    # See DESIGN.md "Per-socket HWM".
    #
    # The per-pump batch caps (BATCH_MSG_CAP / BATCH_BYTE_CAP) are what
    # enforce fairness across the work-stealing pumps. Without them, the
    # first pump that wakes up would drain the entire queue in one
    # non-blocking burst before any other pump got a turn (TCP send
    # buffers absorb the bursts without forcing a fiber yield). 1 MB is
    # generous enough that large-message workloads still batch naturally
    # (16+ × 64 KB messages per batch) while keeping per-pump latency
    # bounded.
    #
    class Push
      BATCH_MSG_CAP  = 256
      BATCH_BYTE_CAP = 256 * 1024

      # @param engine [Engine]
      def initialize(engine)
        @engine     = engine
        @send_queue = Async::LimitedQueue.new(engine.options.send_hwm)
        @pumps      = {} # conn => pump task
        @in_flight  = 0  # batches dequeued but not yet flushed
      end


      # @return [Boolean] true once the shared queue is empty AND no
      #   batch is mid-write across any pump.
      def send_queue_drained?
        @send_queue.empty? && @in_flight.zero?
      end


      # User-facing send: enqueue onto the shared queue. Blocks the caller
      # when the queue is full (HWM backpressure).
      #
      # @param body [String]
      # @return [void]
      def send(body)
        @send_queue.enqueue(body)
      end


      # Called by the Engine when a new peer connection is registered.
      # Spawns a send pump fiber for it.
      #
      # @param conn [Connection]
      def connection_added(conn)
        task = @engine.spawn_task(annotation: "nnq send pump #{conn.endpoint}") do
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
          rescue EOFError, IOError, Errno::EPIPE, Errno::ECONNRESET
            # Peer died mid-flush. In-flight batch dropped — PUSH has no
            # cross-peer ordering guarantee.
            break
          end
        ensure
          @engine.handle_connection_lost(conn)
        end
        @pumps[conn] = task
      end


      # Called by the Engine when a peer connection is removed. Does NOT
      # stop the pump task — the pump cleans itself up via the rescue/
      # ensure in #connection_added. Stopping it here would re-enter
      # handle_connection_lost from inside it.
      def connection_removed(conn)
        @pumps.delete(conn)
      end


      # Stops all send pump tasks. Each pump's ensure block calls
      # engine.handle_connection_lost which deletes from @pumps, so
      # snapshot the values before iterating.
      def close
        @pumps.values.each(&:stop)
      end


      private

      def drain_capped(batch)
        bytes = batch[0].bytesize
        while batch.size < BATCH_MSG_CAP && bytes < BATCH_BYTE_CAP
          msg = @send_queue.dequeue(timeout: 0)
          break unless msg
          batch << msg
          bytes += msg.bytesize
        end
      end


      def write_batch(conn, batch)
        if batch.size == 1
          conn.write_message(batch[0])
        else
          batch.each { |body| conn.write_message(body) }
        end
        conn.flush
      end
    end
  end
end
