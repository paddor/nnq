# frozen_string_literal: true

require "async"

module NNQ
  module Send
    # Coalesces concurrent sends into one writev syscall, *without* an HWM.
    #
    # Each call to {#commit} enqueues a (bytes, promise) pair into a small
    # in-memory @pending array, then either:
    #
    #   - becomes the drainer (if no one else is draining), writes everything
    #     in @pending to the underlying connection, flushes once, resolves
    #     each promise, then loops to pick up anything that arrived during
    #     the flush; or
    #
    #   - blocks on its own promise until the current drainer reaches it.
    #
    # Backpressure model: the only thing that can stop the drainer is the
    # underlying `connection.flush` blocking on `wait_writable` because the
    # OS socket buffer is full. While the drainer is parked there, new
    # callers pile up in @pending and are paused on their promises. The
    # @pending array is unbounded by configuration but bounded by "how many
    # fibers/threads happen to be calling commit at this moment", which is
    # in turn bounded by the user's program structure — not by an HWM knob.
    #
    # Cancellation: if a fiber blocked on `promise.wait` is stopped before
    # the drainer reaches its message, {#commit} splices the entry out of
    # @pending. If the message is already on the wire, the cancellation
    # loses the race and the message is delivered (best-effort, matching
    # libnng's `nng_aio_cancel` semantics).
    #
    # The {#flush_one} method is the unit-test seam: tests substitute their
    # own block to inspect what would be flushed.
    #
    class Staging
      # @param connection [#write_message, #flush] anything that can stage
      #   bytes and flush. In production, this is a Protocol::SP::Connection.
      def initialize(connection)
        @connection = connection
        @pending    = []
        @draining   = false
        @mutex      = Mutex.new
      end


      # Enqueues +bytes+ for delivery and blocks the calling fiber until
      # the message is on the wire (or its delivery is reported as failed).
      #
      # @param bytes [String]
      # @return [void]
      # @raise [Exception] whatever the connection raised during flush
      def commit(bytes)
        promise = Async::Promise.new
        entry   = [bytes, promise]
        @mutex.synchronize { @pending << entry }
        try_drain
        begin
          promise.wait
        rescue Async::Stop, Interrupt
          @mutex.synchronize { @pending.delete(entry) }
          raise
        end
      end


      private

      # Attempts to become the drainer. Exactly one fiber is the drainer
      # at any given moment; everyone else returns immediately.
      def try_drain
        @mutex.synchronize do
          return if @draining
          @draining = true
        end

        loop do
          batch = @mutex.synchronize do
            taken = @pending
            @pending = []
            if taken.empty?
              @draining = false
              nil
            else
              taken
            end
          end
          break if batch.nil?

          begin
            batch.each { |bytes, _| @connection.write_message(bytes) }
            @connection.flush
          rescue => e
            batch.each { |_, promise| promise.reject(e) }
            @mutex.synchronize { @draining = false }
            raise
          end

          batch.each { |_, promise| promise.resolve(nil) }
        end
      end
    end
  end
end
