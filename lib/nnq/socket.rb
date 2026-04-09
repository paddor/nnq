# frozen_string_literal: true

require "async/queue"

require_relative "options"
require_relative "engine"
require_relative "monitor_event"
require_relative "reactor"

module NNQ
  # Socket base class. Subclasses (PUSH, PULL, ...) wire up a routing
  # strategy and the SP protocol id.
  #
  class Socket
    # @return [Options]
    attr_reader :options


    def self.bind(endpoint, **opts)
      sock = new(**opts)
      sock.bind(endpoint)
      sock
    end


    def self.connect(endpoint, **opts)
      sock = new(**opts)
      sock.connect(endpoint)
      sock
    end


    def initialize(linger: nil, send_hwm: Options::DEFAULT_HWM)
      @options = Options.new(linger: linger, send_hwm: send_hwm)
      @engine  = Engine.new(protocol: protocol, options: @options) { |engine| build_routing(engine) }
    end


    def bind(endpoint)
      ensure_parent_task
      Reactor.run { @engine.bind(endpoint) }
    end


    def connect(endpoint)
      ensure_parent_task
      Reactor.run { @engine.connect(endpoint) }
    end


    def close
      Reactor.run { @engine.close }
      nil
    end


    def last_endpoint = @engine.last_endpoint


    def connection_count = @engine.connections.size


    # Resolves with the first connected peer (or nil on close without
    # any peers). Block on `.wait` to wait until a connection is ready.
    def peer_connected = @engine.peer_connected


    # Resolves with `true` the first time all peers have disconnected
    # (after at least one peer was connected). Edge-triggered.
    def all_peers_gone = @engine.all_peers_gone


    def reconnect_enabled  = @engine.reconnect_enabled
    def reconnect_enabled=(value)
      @engine.reconnect_enabled = value
    end


    # Closes the recv side only. Buffered messages drain, then
    # {#receive} returns nil. Send side stays open.
    def close_read
      Reactor.run { @engine.close_read }
      nil
    end


    # Yields lifecycle events for this socket until it's closed or
    # the returned task is stopped.
    #
    # @param verbose [Boolean] when true, also emits :message_sent /
    #   :message_received events
    # @yield [event]
    # @yieldparam event [MonitorEvent]
    # @return [Async::Task]
    def monitor(verbose: false, &block)
      ensure_parent_task
      queue = Async::Queue.new
      @engine.monitor_queue   = queue
      @engine.verbose_monitor = verbose
      Reactor.run do
        @engine.spawn_task(annotation: "nnq monitor") do
          while (event = queue.dequeue)
            block.call(event)
          end
        rescue Async::Stop
        ensure
          @engine.monitor_queue = nil
          block.call(MonitorEvent.new(type: :monitor_stopped))
        end
      end
    end


    private

    def ensure_parent_task
      # Must run OUTSIDE Reactor.run so that non-Async callers capture
      # the IO thread's root task, not the ephemeral work-item task
      # that Reactor wraps each dispatched block in. Inside an Async
      # reactor, the current task is the right parent.
      if Async::Task.current?
        @engine.capture_parent_task(Async::Task.current, on_io_thread: false)
      else
        @engine.capture_parent_task(Reactor.root_task, on_io_thread: true)
      end
    end


    # Subclass hooks.

    def protocol
      raise NotImplementedError
    end


    def build_routing(_engine)
      raise NotImplementedError
    end
  end
end
