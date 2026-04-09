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
    def peer_protocol = @sp.peer_protocol


    # Writes one message into the SP connection's send buffer (no flush).
    #
    # @param body [String]
    # @return [void]
    def write_message(body)
      raise ClosedError, "connection closed" if @closed
      @sp.write_message(body)
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
    def closed? = @closed


    # Closes the underlying SP connection. Safe to call twice.
    def close
      return if @closed
      @closed = true
      @sp.close
    end
  end
end
