# frozen_string_literal: true

require "async"
require "async/clock"
require "set"
require "protocol/sp"
require_relative "error"
require_relative "connection"
require_relative "monitor_event"
require_relative "reactor"
require_relative "engine/socket_lifecycle"
require_relative "engine/connection_lifecycle"
require_relative "engine/reconnect"
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


    # @return [Routing strategy]
    attr_reader :routing


    # @return [Hash{NNQ::Connection => ConnectionLifecycle}]
    attr_reader :connections


    # @return [SocketLifecycle]
    attr_reader :lifecycle


    # @return [String, nil]
    attr_reader :last_endpoint


    # @return [Async::Condition] signaled when a new pipe is registered
    attr_reader :new_pipe


    # @return [Set<String>] endpoints we have called #connect on; used
    #   to decide whether to schedule a reconnect after a connection
    #   is lost.
    attr_reader :dialed


    # @return [Async::Queue, nil] monitor event queue (set by Socket#monitor)
    attr_accessor :monitor_queue


    # @return [Boolean] when true, {#emit_verbose_monitor_event} forwards
    #   per-message traces (:message_sent / :message_received) to the
    #   monitor queue. Set by {Socket#monitor} via its +verbose:+ kwarg.
    attr_accessor :verbose_monitor


    # @param protocol [Integer] our SP protocol id (e.g. Protocols::PUSH_V0)
    # @param options [Options]
    # @yieldparam engine [Engine] used by the caller to build a routing
    #   strategy with access to the engine's connection map
    def initialize(protocol:, options:)
      @protocol        = protocol
      @options         = options
      @connections     = {}
      @listeners       = []
      @lifecycle       = SocketLifecycle.new
      @last_endpoint   = nil
      @new_pipe        = Async::Condition.new
      @monitor_queue   = nil
      @verbose_monitor = false
      @dialed          = Set.new
      @routing         = yield(self)
    end


    # Emits a monitor event to the attached queue (if any).
    def emit_monitor_event(type, endpoint: nil, detail: nil)
      return unless @monitor_queue
      @monitor_queue.enqueue(MonitorEvent.new(type: type, endpoint: endpoint, detail: detail))
    rescue Async::Stop
    end


    # Emits a :message_sent verbose event. Early-returns before
    # allocating the detail hash so the hot send path pays nothing
    # when verbose monitoring is off.
    def emit_verbose_msg_sent(body)
      return unless @verbose_monitor
      emit_monitor_event(:message_sent, detail: { body: body })
    end


    # Emits a :message_received verbose event. Same early-return
    # discipline as {#emit_verbose_msg_sent}.
    def emit_verbose_msg_received(body)
      return unless @verbose_monitor
      emit_monitor_event(:message_received, detail: { body: body })
    end


    # @return [Async::Task, nil]
    def parent_task
      @lifecycle.parent_task
    end


    # @return [Async::Barrier, nil]
    def barrier
      @lifecycle.barrier
    end


    def closed?
      @lifecycle.closed?
    end


    # @return [Async::Promise] resolves with the first connected peer
    def peer_connected
      @lifecycle.peer_connected
    end


    # @return [Async::Promise] resolves when all peers have disconnected
    #   (edge-triggered, after at least one peer connected)
    def all_peers_gone
      @lifecycle.all_peers_gone
    end


    # Called by ConnectionLifecycle teardown. Resolves `all_peers_gone`
    # if the connection set is now empty and we had peers.
    def resolve_all_peers_gone_if_empty
      @lifecycle.resolve_all_peers_gone_if_empty(@connections)
    end


    # @return [Boolean]
    def reconnect_enabled
      @lifecycle.reconnect_enabled
    end


    # Disables or re-enables automatic reconnect. nnq has no reconnect
    # loop yet, so this is forward-looking — {TransientMonitor} flips
    # it before draining.
    def reconnect_enabled=(value)
      @lifecycle.reconnect_enabled = value
    end


    # Closes only the recv side. Buffered messages drain, then
    # {Socket#receive} returns nil. Send side remains operational.
    def close_read
      @routing.close_read if @routing.respond_to?(:close_read)
    end


    # Stores the parent Async task that long-lived NNQ fibers will
    # attach to. The caller (Socket) is responsible for picking the
    # right one (the user's current task, or Reactor.root_task).
    def capture_parent_task(task, on_io_thread:)
      @lifecycle.capture_parent_task(task, on_io_thread: on_io_thread)
    end


    # Binds to +endpoint+. Synchronous: errors propagate.
    def bind(endpoint)
      transport = transport_for(endpoint)
      listener  = transport.bind(endpoint, self)
      listener.start_accept_loop(@lifecycle.barrier) do |io, framing = :tcp|
        handle_accepted(io, endpoint: endpoint, framing: framing)
      end
      @listeners << listener
      @last_endpoint = listener.endpoint
      emit_monitor_event(:listening, endpoint: @last_endpoint)
    end


    # Connects to +endpoint+. Non-blocking for tcp:// and ipc:// — the
    # actual dial happens inside a background reconnect task that
    # retries with exponential back-off until the peer becomes
    # reachable. Inproc connect is synchronous and instant.
    def connect(endpoint)
      @dialed << endpoint
      @last_endpoint = endpoint

      if endpoint.start_with?("inproc://")
        transport_for(endpoint).connect(endpoint, self)
      else
        emit_monitor_event(:connect_delayed, endpoint: endpoint)
        Reconnect.schedule(endpoint, @options, @lifecycle.barrier, self, delay: 0)
      end
    end


    # Schedules a reconnect for +endpoint+ if auto-reconnect is enabled
    # and the endpoint is still in the dialed set. Called from the
    # connection lifecycle's `lost!` path.
    def maybe_reconnect(endpoint)
      return unless endpoint && @dialed.include?(endpoint)
      return unless @lifecycle.alive? && @lifecycle.reconnect_enabled
      return if endpoint.start_with?("inproc://")
      Reconnect.schedule(endpoint, @options, @lifecycle.barrier, self)
    end


    # Public so {Reconnect} can dial directly without re-deriving the
    # transport from the URL each iteration.
    def transport_for(endpoint)
      scheme = endpoint[/\A([a-z+]+):\/\//i, 1] or raise Error, "no scheme: #{endpoint}"
      TRANSPORTS[scheme] or raise Error, "unsupported transport: #{scheme}"
    end


    # Called by transports for each accepted client connection.
    def handle_accepted(io, endpoint:, framing: :tcp)
      lifecycle = ConnectionLifecycle.new(self, endpoint: endpoint, framing: framing)
      lifecycle.handshake!(io)
      spawn_recv_loop(lifecycle.conn) if @routing.respond_to?(:enqueue) && @connections.key?(lifecycle.conn)
      lifecycle.start_supervisor!
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
      lifecycle.start_supervisor!
    rescue ConnectionRejected
      # unusual on connect side, but handled identically
    end


    # Spawns a task under the given parent barrier (defaults to the
    # socket-level barrier). Used by routing strategies (e.g. PUSH send
    # pump) to attach long-lived fibers to the engine's lifecycle. The
    # parent barrier tracks every spawned task so teardown is a single
    # barrier.stop call.
    def spawn_task(annotation:, parent: @lifecycle.barrier, &block)
      parent.async(annotation: annotation, &block)
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

      # Cascade-cancel every remaining task (reconnect loops, accept
      # loops, supervisors) in one shot.
      @lifecycle.barrier&.stop
      @lifecycle.finish_closing!
      @new_pipe.signal

      # Unblock anyone waiting on peer_connected when the socket is
      # closed before a peer ever arrived.
      @lifecycle.peer_connected.resolve(nil) unless @lifecycle.peer_connected.resolved?
      emit_monitor_event(:closed)
      close_monitor_queue
    end


    # Called by routing pumps (or the recv loop) when their connection
    # has died. Idempotent via the lifecycle state guard.
    def handle_connection_lost(conn)
      @connections[conn]&.lost!
    end


    private


    def close_monitor_queue
      return unless @monitor_queue
      @monitor_queue.enqueue(nil)
    end


    def drain_send_queue(timeout)
      return unless @routing.respond_to?(:send_queue_drained?)
      return if @connections.empty?

      deadline = timeout ? Async::Clock.now + timeout : nil

      until @routing.send_queue_drained?
        break if deadline && (deadline - Async::Clock.now) <= 0
        sleep 0.001
      end
    rescue Async::Stop
      # Parent task is being cancelled — stop draining and let close
      # proceed with the rest of teardown instead of propagating the
      # cancellation out of the ensure path.
    end


    def spawn_recv_loop(conn)
      @connections[conn].barrier.async(annotation: "nnq recv #{conn.endpoint}") do
        loop do
          body = conn.receive_message
          emit_verbose_msg_received(body)
          @routing.enqueue(body, conn)
        rescue *CONNECTION_LOST, Async::Stop
          break
        end
      end
    end

  end
end
