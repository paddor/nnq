# frozen_string_literal: true

require "async/queue"

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


    # @yieldparam [self] the socket; when a block is passed the socket
    #   is {#close}d when the block returns (or raises), File.open-style.
    def initialize(raw: false, linger: Float::INFINITY, send_hwm: Options::DEFAULT_HWM, recv_hwm: Options::DEFAULT_HWM)
      @raw     = raw
      @options = Options.new(linger: linger, send_hwm: send_hwm, recv_hwm: recv_hwm)
      @engine  = Engine.new(protocol: protocol, options: @options) { |engine| build_routing(engine) }

      begin
        yield self
      ensure
        close
      end if block_given?
    end


    def raw?
      @raw
    end


    def bind(endpoint, **opts)
      ensure_parent_task
      Reactor.run { @engine.bind(endpoint, **opts) }
    end


    def connect(endpoint, **opts)
      ensure_parent_task
      Reactor.run { @engine.connect(endpoint, **opts) }
    end


    def close
      Reactor.run { @engine.close }
      nil
    end


    def last_endpoint
      @engine.last_endpoint
    end


    def connection_count
      @engine.connections.size
    end


    # Resolves with the first connected peer (or nil on close without
    # any peers). Block on `.wait` to wait until a connection is ready.
    def peer_connected
      @engine.peer_connected
    end


    # Resolves with `true` the first time all peers have disconnected
    # (after at least one peer was connected). Edge-triggered.
    def all_peers_gone
      @engine.all_peers_gone
    end


    def reconnect_enabled
      @engine.reconnect_enabled
    end


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

      queue                   = Async::Queue.new
      @engine.monitor_queue   = queue
      @engine.verbose_monitor = verbose

      Reactor.run do
        @engine.monitor_task = @engine.spawn_task(annotation: "nnq monitor") do
          while (event = queue.dequeue)
            block.call(event)
          end
        rescue Async::Stop
        ensure
          @engine.monitor_queue = nil
          @engine.monitor_task  = nil
          block.call(MonitorEvent.new(type: :monitor_stopped))
        end
      end
    end


    # Coerces +body+ to a frozen `Encoding::BINARY`-tagged String and
    # returns it. Every send method runs its body through this so the
    # receiver sees a uniform frozen+BINARY contract across transports
    # (mutation bugs raise `FrozenError` instead of silently corrupting
    # a shared reference on the inproc fast path).
    #
    # Fast-path: unfrozen non-BINARY strings are re-tagged in place
    # (force_encoding is a flag flip, no copy). The pathological case
    # of a frozen non-BINARY body (e.g. a `# frozen_string_literal: true`
    # literal) can't be re-tagged in place — the inproc {Pipe} handles
    # that with a copy so the receive contract stays uniform.
    def coerce_binary(body)
      body = body.to_str unless body.is_a?(String)
      body.force_encoding(Encoding::BINARY) unless body.frozen? || body.encoding == Encoding::BINARY
      body.freeze
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
