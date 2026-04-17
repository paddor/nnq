# frozen_string_literal: true

require "socket"
require "io/stream"

module NNQ
  # Lifecycle event emitted by {Socket#monitor}.
  #
  # @!attribute [r] type
  #   @return [Symbol] event type (:listening, :connected, :disconnected, ...)
  # @!attribute [r] endpoint
  #   @return [String, nil] the endpoint involved
  # @!attribute [r] detail
  #   @return [Hash, nil] extra context
  #
  MonitorEvent = Data.define(:type, :endpoint, :detail) do
    def initialize(type:, endpoint: nil, detail: nil)
      super
    end
  end


  # Errors that indicate an established connection went away. Used by
  # the recv loop, routing pumps, and connection lifecycle to silently
  # terminate (the connection lifecycle's #lost! handler decides
  # whether to reconnect). Not frozen at load time — transport plugins
  # append to this before the first bind/connect, which freezes both
  # arrays.
  CONNECTION_LOST = [
    EOFError,
    IOError,
    Errno::ECONNRESET,
    Errno::EPIPE,
  ]


  # Errors raised when a peer cannot be reached. Triggers a reconnect
  # retry rather than propagating.
  CONNECTION_FAILED = [
    Errno::ECONNREFUSED,
    Errno::EHOSTUNREACH,
    Errno::ENETUNREACH,
    Errno::ENOENT,
    Errno::EPIPE,
    Errno::ETIMEDOUT,
    Socket::ResolutionError,
  ]


  # Freezes module-level state so NNQ sockets can be used inside Ractors.
  # Call this once before spawning any Ractors that create NNQ sockets.
  #
  def self.freeze_for_ractors!
    CONNECTION_LOST.freeze
    CONNECTION_FAILED.freeze
    Engine.transports.freeze
  end
end
