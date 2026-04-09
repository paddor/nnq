# frozen_string_literal: true

require "protocol/sp"
require_relative "send/staging"

module NNQ
  # Per-pipe state: a Protocol::SP::Connection plus a Send::Staging.
  #
  # Owns no fibers itself — the recv loop and reconnect logic are driven
  # by the Engine. This class is the smallest unit shared between PUSH,
  # PULL, PAIR, etc.
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
      @staging  = Send::Staging.new(sp)
      @closed   = false
    end


    # @return [Integer] peer protocol id (e.g. Protocols::PULL_V0)
    def peer_protocol = @sp.peer_protocol


    # Stages +body+ for delivery and blocks until on-wire (or rejected).
    #
    # @param body [String]
    # @return [void]
    def send_message(body)
      raise ClosedError, "connection closed" if @closed
      @staging.commit(body)
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
