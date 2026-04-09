# frozen_string_literal: true

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


      # @param engine [Engine]
      # @param endpoint [String, nil]
      # @param framing [Symbol] :tcp or :ipc
      def initialize(engine, endpoint:, framing:)
        @engine   = engine
        @endpoint = endpoint
        @framing  = framing
        @state    = :new
        @conn     = nil
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
        sp.handshake!
        ready!(NNQ::Connection.new(sp, endpoint: @endpoint))
        @conn
      rescue => e
        @engine.emit_monitor_event(:handshake_failed, endpoint: @endpoint, detail: { error: e })
        io.close rescue nil
        transition!(:closed) unless @state == :closed
        raise
      end


      # Unexpected loss of an established connection. Tears down and
      # asks the engine to schedule a reconnect (if the endpoint is in
      # the dialed set and reconnect is still enabled).
      def lost!
        ep = @endpoint
        tear_down!
        @engine.maybe_reconnect(ep)
      end


      # Deliberate close (engine shutdown or routing eviction). Does
      # not trigger reconnect.
      def close!
        tear_down!
      end


      private

      def ready!(conn)
        @conn                    = conn
        @engine.connections[conn] = self
        transition!(:ready)
        begin
          @engine.routing.connection_added(conn) if @engine.routing.respond_to?(:connection_added)
        rescue ConnectionRejected
          @engine.emit_monitor_event(:connection_rejected, endpoint: @endpoint)
          tear_down!
          raise
        end
        @engine.lifecycle.peer_connected.resolve(conn) unless @engine.lifecycle.peer_connected.resolved?
        @engine.emit_monitor_event(:handshake_succeeded, endpoint: @endpoint)
        @engine.emit_monitor_event(:connected, endpoint: @endpoint)
        @engine.new_pipe.signal
      end


      def tear_down!
        return if @state == :closed
        transition!(:closed)
        if @conn
          @engine.connections.delete(@conn)
          @engine.routing.connection_removed(@conn) if @engine.routing.respond_to?(:connection_removed)
          @conn.close rescue nil
          @engine.emit_monitor_event(:disconnected, endpoint: @endpoint)
          @engine.resolve_all_peers_gone_if_empty
        end
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
