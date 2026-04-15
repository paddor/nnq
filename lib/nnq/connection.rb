# frozen_string_literal: true

require "protocol/sp"

module NNQ
  # Per-pipe state: thin wrapper around Protocol::SP::Connection.
  #
  # Owns no fibers itself — recv loop and send pump are spawned by
  # the Engine and routing strategy respectively.
  #
  class Connection
    # @return [Protocol::SP::Connection]
    attr_reader :sp


    # @return [String, nil] endpoint URI we connected to / accepted from
    attr_reader :endpoint


    # @param sp [Protocol::SP::Connection] handshake-completed SP connection
    # @param endpoint [String, nil]
    def initialize(sp, endpoint: nil)
      @sp       = sp
      @endpoint = endpoint
      @closed   = false
    end


    # @return [Integer] peer protocol id (e.g. Protocols::PULL_V0)
    def peer_protocol
      @sp.peer_protocol
    end


    # Writes one message into the SP connection's send buffer (no flush).
    #
    # @param body [String]
    # @param header [String, nil] optional binary prefix written between
    #   the SP length prefix and body (see Protocol::SP::Connection)
    # @return [void]
    def write_message(body, header: nil)
      raise ClosedError, "connection closed" if @closed
      @sp.write_message(body, header: header)
    end


    # Writes a batch of bodies under a single SP mutex acquisition.
    # Used by the work-stealing send pump hot path.
    #
    # @param bodies [Array<String>]
    # @return [void]
    def write_messages(bodies)
      raise ClosedError, "connection closed" if @closed
      @sp.write_messages(bodies)
    end


    # Writes one message AND flushes immediately. Used by REQ/REP where
    # each call is request-paced and there's nothing to batch.
    #
    # @param body [String]
    # @param header [String, nil] optional binary prefix
    # @return [void]
    def send_message(body, header: nil)
      raise ClosedError, "connection closed" if @closed
      @sp.send_message(body, header: header)
    end


    # Flushes the SP connection's send buffer to the socket.
    #
    # @return [void]
    def flush
      @sp.flush
    end


    # Reads one message body off the wire. Blocks the calling fiber.
    #
    # @return [String]
    def receive_message
      @sp.receive_message
    end


    # @return [Boolean]
    def closed?
      @closed
    end


    # Closes the underlying SP connection. Safe to call twice.
    def close
      return if @closed
      @closed = true
      @sp.close
    end

  end
end
