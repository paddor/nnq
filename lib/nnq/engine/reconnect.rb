# frozen_string_literal: true

module NNQ
  class Engine
    # Connection errors that should trigger a reconnect retry rather
    # than propagate. Mutable at load time so plugins (e.g. a future
    # TLS transport) can append their own error classes; frozen on
    # first {Engine#connect}.
    CONNECTION_FAILED = [
      Errno::ECONNREFUSED,
      Errno::EHOSTUNREACH,
      Errno::ENETUNREACH,
      Errno::ENOENT,
      Errno::EPIPE,
      Errno::ETIMEDOUT,
      Socket::ResolutionError,
    ]

    # Errors that indicate an established connection went away. Used
    # by the recv loop and pumps to silently terminate (the connection
    # lifecycle's #lost! handler decides whether to reconnect).
    CONNECTION_LOST = [
      EOFError,
      IOError,
      Errno::ECONNRESET,
      Errno::EPIPE,
    ]


    # Schedules reconnect attempts with exponential back-off.
    #
    # Runs a background task that loops until a connection is
    # established or the engine is closed. Caller is non-blocking:
    # {Engine#connect} returns immediately and the actual dial happens
    # inside the task.
    #
    class Reconnect
      # @param endpoint [String]
      # @param options [Options]
      # @param parent_task [Async::Task]
      # @param engine [Engine]
      # @param delay [Numeric, nil] initial delay (defaults to reconnect_interval)
      def self.schedule(endpoint, options, parent_task, engine, delay: nil)
        new(engine, endpoint, options).run(parent_task, delay: delay)
      end


      def initialize(engine, endpoint, options)
        @engine   = engine
        @endpoint = endpoint
        @options  = options
      end


      def run(parent_task, delay: nil)
        delay, max_delay = init_delay(delay)

        parent_task.async(transient: true, annotation: "nnq reconnect #{@endpoint}") do
          loop do
            break if @engine.closed?
            sleep quantized_wait(delay) if delay > 0
            break if @engine.closed?
            begin
              @engine.transport_for(@endpoint).connect(@endpoint, @engine)
              break
            rescue *CONNECTION_FAILED, *CONNECTION_LOST => e
              delay = next_delay(delay, max_delay)
              @engine.emit_monitor_event(:connect_retried, endpoint: @endpoint, detail: { interval: delay, error: e })
            end
          end
        rescue Async::Stop
        end
      end


      private


      # Wall-clock quantized sleep: wait until the next +delay+-sized
      # grid tick. Multiple clients reconnecting with the same interval
      # wake up at the same instant, collapsing staggered retries into
      # aligned waves.
      def quantized_wait(delay, now = Time.now.to_f)
        wait = delay - (now % delay)
        wait.positive? ? wait : delay
      end


      def init_delay(delay)
        ri = @options.reconnect_interval
        if ri.is_a?(Range)
          [delay || ri.begin, ri.end]
        else
          [delay || ri, nil]
        end
      end


      def next_delay(delay, max_delay)
        ri = @options.reconnect_interval
        if ri.is_a?(Range)
          delay = delay * 2
          delay = [delay, max_delay].min if max_delay
          delay = ri.begin if delay == 0
          delay
        else
          ri
        end
      end
    end
  end
end
