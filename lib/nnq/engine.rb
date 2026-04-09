# frozen_string_literal: true

require "async"
require "async/clock"
require "protocol/sp"
require_relative "connection"
require_relative "reactor"
require_relative "transport/tcp"

module NNQ
  # Per-socket orchestrator. Owns a parent Async task, the connection
  # array (mutated in place so routing strategies can hold a stable
  # `cycle` enumerator), the transport registry, and the lifecycle of
  # accepted/dialed pipes.
  #
  # Mirrors omq's Engine in shape but is much smaller because there's
  # no HWM, no per-pipe queues, no command frames, no mechanisms.
  #
  class Engine
    TRANSPORTS = {
      "tcp" => Transport::TCP,
    }


    # @return [Options]
    attr_reader :options

    # @return [Array<Connection>] mutated in place; routing holds a stable cycle on it
    attr_reader :connections

    # @return [Async::Task, nil]
    attr_reader :parent_task

    # @return [String, nil]
    attr_reader :last_endpoint


    # @return [Async::Condition] signaled when a new pipe is registered
    attr_reader :new_pipe


    # @param protocol [Integer] our SP protocol id (e.g. Protocols::PUSH_V0)
    # @param options [Options]
    # @yieldparam engine [Engine] used by the caller to build a routing
    #   strategy with access to the engine's stable connection array
    def initialize(protocol:, options:)
      @protocol      = protocol
      @options       = options
      @connections   = []
      @listeners     = []
      @parent_task   = nil
      @last_endpoint = nil
      @new_pipe      = Async::Condition.new
      @closed        = false
      @routing       = yield(self)
    end


    # Stores the parent Async task that long-lived NNQ fibers will
    # attach to. The caller (Socket) is responsible for picking the
    # right one (the user's current task, or Reactor.root_task).
    def capture_parent_task(task)
      @parent_task ||= task
    end


    # Binds to +endpoint+. Synchronous: errors propagate.
    def bind(endpoint)
      transport = transport_for(endpoint)
      listener  = transport.bind(endpoint, self)
      listener.start_accept_loop(@parent_task) do |io|
        handle_accepted(io, endpoint: endpoint)
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
    def handle_accepted(io, endpoint:)
      sp = Protocol::SP::Connection.new(io, protocol: @protocol, max_message_size: @options.max_message_size)
      sp.handshake!
      register(Connection.new(sp, endpoint: endpoint))
    rescue => e
      io.close rescue nil
      warn("nnq: handshake failed for #{endpoint}: #{e.class}: #{e.message}") if $DEBUG
    end


    # Called by transports for each dialed connection.
    def handle_connected(io, endpoint:)
      sp = Protocol::SP::Connection.new(io, protocol: @protocol, max_message_size: @options.max_message_size)
      sp.handshake!
      register(Connection.new(sp, endpoint: endpoint))
    end


    # @return [Routing strategy]
    attr_reader :routing


    # Spawns a task under the engine's parent task. Used by routing
    # strategies (e.g. PUSH send pump) to attach long-lived fibers to
    # the engine's lifecycle without going through transient: true.
    #
    # @param annotation [String]
    # @yield pump body
    # @return [Async::Task]
    def spawn_task(annotation:, &block)
      @parent_task.async(annotation: annotation, &block)
    end


    # Closes the engine: stops listeners, drains the send queue subject
    # to linger, stops routing pumps (which by now are parked on the
    # empty queue), then closes connections (which wakes recv loops
    # with EOF). Order matters — closing connections first would force
    # mid-flush pumps to abort with IOError.
    def close
      return if @closed
      @listeners.each(&:stop)
      drain_send_queue(@options.linger)
      @routing.close if @routing.respond_to?(:close)
      @closed = true
      @connections.each(&:close)
      @new_pipe.signal
    end


    # Called by routing pumps (or the recv loop) when their connection
    # has died. Idempotent.
    def handle_connection_lost(conn)
      return unless @connections.delete(conn)
      @routing.connection_removed(conn) if @routing.respond_to?(:connection_removed)
      conn.close
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


    def register(conn)
      @connections << conn
      @routing.connection_added(conn) if @routing.respond_to?(:connection_added)
      # connection_added may have rejected the conn (e.g. PAIR's
      # first-pipe-wins) and routed it through handle_connection_lost,
      # which removes it from @connections. Don't spawn a recv loop in
      # that case.
      return unless @connections.include?(conn)
      spawn_recv_loop(conn) if @routing.respond_to?(:enqueue)
      @new_pipe.signal
    end


    def spawn_recv_loop(conn)
      @parent_task.async(annotation: "nnq recv #{conn.endpoint}") do
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
