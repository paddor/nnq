# frozen_string_literal: true

require_relative "options"
require_relative "engine"
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


    private

    def ensure_parent_task
      # Must run OUTSIDE Reactor.run so that non-Async callers capture
      # the IO thread's root task, not the ephemeral work-item task
      # that Reactor wraps each dispatched block in. Inside an Async
      # reactor, the current task is the right parent.
      if Async::Task.current?
        @engine.capture_parent_task(Async::Task.current)
      else
        @engine.capture_parent_task(Reactor.root_task)
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
