# frozen_string_literal: true

module NNQ
  class Engine
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
              @engine.transport_for(@endpoint).connect(@endpoint, @engine, **@engine.dial_opts_for(@endpoint))
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
