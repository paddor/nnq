# frozen_string_literal: true

require "async/barrier"
require "protocol/sp"
require_relative "../connection"

module NNQ
  class Engine
    # Owns the full arc of one connection: handshake → ready → closed.
    #
    # Centralizes the ordering of side effects (routing registration,
    # teardown) so the sequence lives in one place instead of being
    # scattered across Engine, the accept/connect paths, and the
    # recv/send pumps.
    #
    # State machine:
    #
    #     new → handshaking → ready → closed
    #
    # #lost! and #close! are idempotent — the state guard ensures side
    # effects run exactly once even if multiple pumps race to report a
    # lost connection.
    #
    class ConnectionLifecycle
      class InvalidTransition < RuntimeError; end

      STATES = %i[new handshaking ready closed].freeze

      TRANSITIONS = {
        new:         %i[handshaking ready closed].freeze,
        handshaking: %i[ready closed].freeze,
        ready:       %i[closed].freeze,
        closed:      [].freeze,
      }.freeze


      # @return [NNQ::Connection, nil]
      attr_reader :conn

      # @return [String, nil]
      attr_reader :endpoint

      # @return [Symbol]
      attr_reader :state

      # @return [Async::Barrier] holds all per-connection pump tasks
      #   (send pump, recv pump). When the connection is torn down,
      #   {#tear_down!} calls `@barrier.stop` to cancel every sibling
      #   task atomically.
      attr_reader :barrier


      # @param engine [Engine]
      # @param endpoint [String, nil]
      # @param framing [Symbol] :tcp or :ipc
      def initialize(engine, endpoint:, framing:)
        @engine   = engine
        @endpoint = endpoint
        @framing  = framing
        @state    = :new
        @conn     = nil
        @barrier  = Async::Barrier.new(parent: engine.barrier)
      end


      # Performs the SP handshake, wraps the result in NNQ::Connection,
      # registers with the engine and routing, and transitions to :ready.
      #
      # @param io [#read, #write, #close]
      # @return [NNQ::Connection]
      def handshake!(io)
        transition!(:handshaking)
        sp = Protocol::SP::Connection.new(
          io,
          protocol:         @engine.protocol,
          max_message_size: @engine.options.max_message_size,
          framing:          @framing,
        )
        Async::Task.current.with_timeout(handshake_timeout) { sp.handshake! }
        ready!(NNQ::Connection.new(sp, endpoint: @endpoint))
        @conn
      rescue Protocol::SP::Error, *CONNECTION_LOST, Async::TimeoutError => error
        @engine.emit_monitor_event(:handshake_failed, endpoint: @endpoint, detail: { error: error })
        io.close rescue nil
        # Full tear-down with reconnect: without this, the endpoint
        # goes dead when a peer RSTs mid-handshake.
        tear_down!(reconnect: true)
        raise
      end


      # Unexpected loss of an established connection. Tears down and
      # asks the engine to schedule a reconnect (if the endpoint is in
      # the dialed set and reconnect is still enabled).
      def lost!
        tear_down!(reconnect: true)
      end


      # Deliberate close (engine shutdown or routing eviction). Does
      # not trigger reconnect.
      def close!
        tear_down!(reconnect: false)
      end


      # Starts a supervisor for this connection. Must be called after
      # all per-connection pumps (recv loop, send pump) have been
      # spawned on the connection barrier. The supervisor blocks until
      # the first pump exits, then runs tear_down! via lost!.
      #
      # Called by Engine#handle_accepted / Engine#handle_connected after
      # spawning the recv loop — routing's connection_added may have
      # already spawned send pumps during ready!, so the barrier is
      # guaranteed non-empty by then.
      def start_supervisor!
        start_supervisor unless @barrier.empty?
      end


      private

      def ready!(conn)
        conn                     = wrap_connection(conn)
        @conn                    = conn
        @engine.connections[conn] = self
        transition!(:ready)
        begin
          @engine.routing.connection_added(conn) if @engine.routing.respond_to?(:connection_added)
        rescue ConnectionRejected
          @engine.emit_monitor_event(:connection_rejected, endpoint: @endpoint)
          tear_down!(reconnect: false)
          raise
        end
        @engine.lifecycle.peer_connected.resolve(conn) unless @engine.lifecycle.peer_connected.resolved?
        @engine.emit_monitor_event(:handshake_succeeded, endpoint: @endpoint)
        @engine.emit_monitor_event(:connected, endpoint: @endpoint)
        @engine.new_pipe.signal
      end


      def tear_down!(reconnect: false)
        return if @state == :closed
        transition!(:closed)
        if @conn
          @engine.connections.delete(@conn)
          @engine.routing.connection_removed(@conn) if @engine.routing.respond_to?(:connection_removed)
          @conn.close rescue nil
          @engine.emit_monitor_event(:disconnected, endpoint: @endpoint)
          @engine.resolve_all_peers_gone_if_empty
        end
        @engine.maybe_reconnect(@endpoint) if reconnect
        # Cancel every sibling pump of this connection. The caller is
        # the supervisor task, which is NOT in the barrier — so there
        # is no self-stop risk.
        @barrier.stop
      end


      # Spawns a supervisor task on the *socket-level* barrier (not the
      # per-connection barrier) that blocks on the first pump to finish
      # and then triggers teardown.
      def start_supervisor
        @engine.barrier.async(transient: true, annotation: "conn supervisor") do
          @barrier.wait { |task| task.wait; break }
        rescue Async::Stop, Async::Cancel
        rescue *CONNECTION_LOST
        ensure
          lost!
        end
      end


      # Post-handshake transport wrap. A transport that implements
      # `wrap_connection(conn)` (e.g. nnq-zstd's zstd+tcp) returns a
      # delegating wrapper that adds a layer (compression, TLS, …)
      # without the engine caring. Unknown or hook-less transports pass
      # through unchanged.
      def wrap_connection(conn)
        return conn unless @endpoint
        transport = @engine.transport_for(@endpoint)
        return conn unless transport.respond_to?(:wrap_connection)
        transport.wrap_connection(conn, @engine)
      end


      # Handshake timeout: same logic as TCP.connect_timeout — derived
      # from reconnect_interval (floor 0.5s). Prevents a hang when the
      # peer accepts the TCP connection but never sends an SP greeting.
      def handshake_timeout
        ri = @engine.options.reconnect_interval
        ri = ri.end if ri.is_a?(Range)
        [ri, 0.5].max
      end


      def transition!(new_state)
        allowed = TRANSITIONS[@state]
        unless allowed&.include?(new_state)
          raise InvalidTransition, "#{@state} → #{new_state}"
        end
        @state = new_state
      end
    end
  end
end
