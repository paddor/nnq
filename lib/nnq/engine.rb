# frozen_string_literal: true

require "async"
require "async/clock"
require "protocol/sp"
require_relative "error"
require_relative "connection"
require_relative "reactor"
require_relative "engine/socket_lifecycle"
require_relative "engine/connection_lifecycle"
require_relative "transport/tcp"
require_relative "transport/ipc"
require_relative "transport/inproc"

module NNQ
  # Per-socket orchestrator. Owns the listener set, the connection map
  # (keyed on NNQ::Connection, with per-connection ConnectionLifecycle
  # values), the transport registry, and the socket-level state machine
  # via {SocketLifecycle}.
  #
  # Mirrors OMQ's Engine in shape but is much smaller because there's
  # no HWM bookkeeping, no mechanisms, no heartbeat, no monitor queue.
  #
  class Engine
    TRANSPORTS = {
      "tcp"    => Transport::TCP,
      "ipc"    => Transport::IPC,
      "inproc" => Transport::Inproc,
    }


    # @return [Integer] our SP protocol id (e.g. Protocols::PUSH_V0)
    attr_reader :protocol

    # @return [Options]
    attr_reader :options

    # @return [Hash{NNQ::Connection => ConnectionLifecycle}]
    attr_reader :connections

    # @return [SocketLifecycle]
    attr_reader :lifecycle

    # @return [String, nil]
    attr_reader :last_endpoint

    # @return [Async::Condition] signaled when a new pipe is registered
    attr_reader :new_pipe


    # @param protocol [Integer] our SP protocol id (e.g. Protocols::PUSH_V0)
    # @param options [Options]
    # @yieldparam engine [Engine] used by the caller to build a routing
    #   strategy with access to the engine's connection map
    def initialize(protocol:, options:)
      @protocol      = protocol
      @options       = options
      @connections   = {}
      @listeners     = []
      @lifecycle     = SocketLifecycle.new
      @last_endpoint = nil
      @new_pipe      = Async::Condition.new
      @routing       = yield(self)
    end


    # @return [Routing strategy]
    attr_reader :routing


    # @return [Async::Task, nil]
    def parent_task = @lifecycle.parent_task


    def closed? = @lifecycle.closed?


    # Stores the parent Async task that long-lived NNQ fibers will
    # attach to. The caller (Socket) is responsible for picking the
    # right one (the user's current task, or Reactor.root_task).
    def capture_parent_task(task)
      on_io_thread = task.equal?(Reactor.root_task)
      @lifecycle.capture_parent_task(task, on_io_thread: on_io_thread)
    end


    # Binds to +endpoint+. Synchronous: errors propagate.
    def bind(endpoint)
      transport = transport_for(endpoint)
      listener  = transport.bind(endpoint, self)
      listener.start_accept_loop(@lifecycle.parent_task) do |io, framing = :tcp|
        handle_accepted(io, endpoint: endpoint, framing: framing)
      end
      @listeners << listener
      @last_endpoint = listener.endpoint
    end


    # Connects to +endpoint+. Synchronous on first attempt; reconnect
    # is wired in Phase 1.1.
    def connect(endpoint)
      transport = transport_for(endpoint)
      transport.connect(endpoint, self)
      @last_endpoint = endpoint
    rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH => e
      raise Error, "could not connect to #{endpoint}: #{e.class}: #{e.message}"
    end


    # Called by transports for each accepted client connection.
    def handle_accepted(io, endpoint:, framing: :tcp)
      lifecycle = ConnectionLifecycle.new(self, endpoint: endpoint, framing: framing)
      lifecycle.handshake!(io)
      spawn_recv_loop(lifecycle.conn) if @routing.respond_to?(:enqueue) && @connections.key?(lifecycle.conn)
    rescue ConnectionRejected
      # routing rejected this peer (e.g. PAIR already bonded) — lifecycle cleaned up
    rescue => e
      warn("nnq: handshake failed for #{endpoint}: #{e.class}: #{e.message}") if $DEBUG
    end


    # Called by transports for each dialed connection.
    def handle_connected(io, endpoint:, framing: :tcp)
      lifecycle = ConnectionLifecycle.new(self, endpoint: endpoint, framing: framing)
      lifecycle.handshake!(io)
      spawn_recv_loop(lifecycle.conn) if @routing.respond_to?(:enqueue) && @connections.key?(lifecycle.conn)
    rescue ConnectionRejected
      # unusual on connect side, but handled identically
    end


    # Spawns a task under the socket's parent task. Used by routing
    # strategies (e.g. PUSH send pump) to attach long-lived fibers to
    # the engine's lifecycle without going through transient: true.
    def spawn_task(annotation:, &block)
      @lifecycle.parent_task.async(annotation: annotation, &block)
    end


    # Closes the engine: stops listeners, drains the send queue subject
    # to linger, stops routing pumps (which by now are parked on the
    # empty queue), then tears down every connection's lifecycle. Order
    # matters — closing connections first would force mid-flush pumps
    # to abort with IOError.
    def close
      return unless @lifecycle.alive?
      @lifecycle.start_closing!
      @listeners.each(&:stop)
      drain_send_queue(@options.linger)
      @routing.close if @routing.respond_to?(:close)
      # Tear down each remaining connection via its lifecycle. The
      # collection mutates during iteration, so snapshot the values.
      @connections.values.each(&:close!)
      @lifecycle.finish_closing!
      @new_pipe.signal
    end


    # Called by routing pumps (or the recv loop) when their connection
    # has died. Idempotent via the lifecycle state guard.
    def handle_connection_lost(conn)
      @connections[conn]&.lost!
    end


    private

    def drain_send_queue(timeout)
      return unless @routing.respond_to?(:send_queue_drained?)
      return if @connections.empty?
      deadline = timeout ? Async::Clock.now + timeout : nil
      until @routing.send_queue_drained?
        break if deadline && (deadline - Async::Clock.now) <= 0
        sleep 0.001
      end
    end


    def spawn_recv_loop(conn)
      @lifecycle.parent_task.async(annotation: "nnq recv #{conn.endpoint}") do
        loop do
          body = conn.receive_message
          @routing.enqueue(body, conn)
        rescue EOFError, IOError, Errno::ECONNRESET, Async::Stop
          break
        end
      ensure
        handle_connection_lost(conn)
      end
    end


    def transport_for(endpoint)
      scheme = endpoint[/\A([a-z+]+):\/\//i, 1] or raise Error, "no scheme: #{endpoint}"
      TRANSPORTS[scheme] or raise Error, "unsupported transport: #{scheme}"
    end
  end
end
